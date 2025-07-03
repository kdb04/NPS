import frappe
import csv
import io
import frappe.utils

def process_file(doc, method):
    if doc.status != "Pending" or not doc.uploaded_file:
        return

    frappe.db.set_value("NPS Importer", doc.name, "status", "Processing") #set status to processing
    
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.uploaded_file})
        file_content = file_doc.get_content()
            
        rows = None
        for delimiter in ['\t', ',', ';']: #test for all delimiters
            f = io.StringIO(file_content)
            reader = csv.reader(f, delimiter=delimiter)
            test_rows = list(reader) #content of csv
            if len(test_rows) > 1 and len(test_rows[0]) > 8:
                rows = test_rows
                break

        if not rows or len(rows) <= 1: #only header in csv file
            frappe.db.set_value("NPS Importer", doc.name, {
                "remark": "No valid data rows found in file",
                "status": "Failed"
            })
            return
        
        data_rows = rows  

        ledger_mapping = {
            "25065": "total_amount",
            "15181": "order_value",
            "30064": "registration_fee", 
            "30063": "comission", 
            "15006": "central_gst",
            "15007": "state_gst", 
            "15008": "integrated_gst",
            "30091": "service_charges"
        }
        
        amounts = {}
        ledger_count = {}
        #date_val = ""
        user_val = ""
        
        for idx, row in enumerate(data_rows):
            if not any(row) or len(row) < 10: 
                continue
                
            try:
                #date_val = row[3].strip()
                ledger_code = row[5].strip()
                amount_val = row[8].strip()
                user_val = row[10].strip()
            
            except IndexError:
                continue

            try:
                amount_float = float(amount_val.replace(',', '').replace(' ', ''))
            except:
                amount_float = 0
                
            if ledger_code in ledger_mapping:
                field_name = ledger_mapping[ledger_code]

                if field_name not in ledger_count:
                    ledger_count[field_name] = 0
                    amounts[field_name] = amount_float
                else:
                    amounts[field_name] = 0 #deals with duplicates
                    
                ledger_count[field_name]+=1

        if not amounts and doc.file_type == "Comparison JV":
            file_doc = frappe.get_doc("File", {"file_url": doc.uploaded_file})
            file_name = file_doc.file_name.lower() if hasattr(file_doc, 'file_name') and file_doc.file_name else ""
            file_content = file_doc.get_content()

            validation_result = validate_against_database(None, doc.file_type, file_name, file_content) #None as this jv entry does not go to jv store table

            if validation_result["is_valid"]:
                comp_doc = frappe.new_doc("NPS Transactions")
                comp_doc.discrepancy_count = 0
                comp_doc.remarks = "No discrepancies found."
                comp_doc.insert(ignore_permissions=True, ignore_mandatory=True)

                frappe.db.set_value("NPS Importer", doc.name, {
                    "remark": f"Comparison completed successfully. Report:{comp_doc.na}",
                    "status": "Success"
                })

            else:
                comp_doc = frappe.new_doc("NPS Transactions")
                comp_doc.discrepancy_count = validation_result.get('total_discrepancies', 0)
                
                # remarks = []
                # if validation_result.get('missing_nps_orders'):
                #     nps_order_ids = [order.get('order_id', '') for order in validation_result['missing_nps_orders']]
                #     remarks.append(f"Missing NPS Orders: {', '.join(nps_order_ids)}")
                # if validation_result.get('missing_agent_orders'):
                #     agent_order_ids = [order.get('order_id', '') for order in validation_result['missing_agent_orders']]
                #     remarks.append(f"Missing Agent Orders: {', '.join(agent_order_ids)}")
                
                # all_remarks = " | ".join(remarks) if remarks else "No discrepancy found."
                # full_diff = validation_result.get('difference', [])
                # if isinstance(full_diff, list):
                #     all_remarks += " | " + " | ".join(full_diff)

                # remarks = []
                # total_discrepancies = validation_result.get('total_discrepancies', 0)

                # remarks.append(f"Total Discrepancies Found: {total_discrepancies}")

                # if validation_result.get('missing_nps_orders'):
                #     nps_order_ids = [order.get('order_id', '') for order in validation_result['missing_nps_orders']]
                #     remarks.append(f"Missing NPS Orders: ({len(nps_order_ids)}):")
                #     remarks.append(', '.join(nps_order_ids))
                #     remarks.append("")

                # if validation_result.get('missing_agent_orders'):
                #     agent_order_ids = [order.get('order_id', '') for order in validation_result['missing_agent_orders']]
                #     remarks.append(f"Missing Agent Orders: ({len(agent_order_ids)}):")
                #     remarks.append(', '.join(agent_order_ids))
                #     remarks.append("")

                # full_diff = validation_result.get('difference', [])
                # if full_diff:
                #     remarks.append("Discrepancies Found:")
                #     if isinstance(full_diff, list):
                #         remarks.extend(full_diff)
                #     elif isinstance(full_diff, str):
                #         remarks.extend(full_diff)

                final_remark = validation_result.get('difference', 'Discrepancy found but no details')
            
                comp_doc.remarks = final_remark
                comp_doc.insert(ignore_permissions=True, ignore_mandatory=True)

                frappe.db.set_value("NPS Importer", doc.name, {
                    "status": "Success",
                    "remark": "Discrepancy log saved."
                })
            return 

        if amounts:
            jv_doc = frappe.new_doc("NPS JV Store")
            
            try:
                # if '/' in date_val:
                #     day, month, year = date_val.split('/')
                #     formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                #     jv_doc.for_date = getdate(formatted_date)
                # else:
                #     jv_doc.for_date = getdate(date_val)
                from_date = getattr(doc, "from_date", None)
                to_date = getattr(doc, "to_date", None)

                if from_date:
                    jv_doc.from_date = from_date
                else:
                    jv_doc.from_date = frappe.utils.today()
                
                if to_date:
                    jv_doc.to_date = to_date
                else:
                    jv_doc.to_date = frappe.utils.today()

            except Exception as date_error:
                frappe.log_error(f"Date parsing error: {str(date_error)}", "NPS Date Parse")
                jv_doc.from_date = frappe.utils.today()
                jv_doc.to_date = frappe.utils.today()
 
            jv_doc.user_id = user_val
            jv_doc.status = "Pending"  # set to pending during processing
            jv_doc.import_ref = doc.name
            
            jv_doc.total_amount = amounts.get("total_amount", 0)
            jv_doc.order_value = amounts.get("order_value", 0)
            jv_doc.registration_fee = amounts.get("registration_fee", 0)
            jv_doc.comission = amounts.get("comission", 0)  
            jv_doc.central_gst = amounts.get("central_gst", 0)
            jv_doc.state_gst = amounts.get("state_gst", 0)
            jv_doc.integrated_gst = amounts.get("integrated_gst", 0)
            jv_doc.service_charges = amounts.get("service_charges", 0)

            jv_doc.insert(ignore_permissions=True, ignore_mandatory=True)

            validation_result = validate_against_database(jv_doc, doc.file_type)
            
            if validation_result["is_valid"]:
                frappe.db.set_value("NPS JV Store", jv_doc.name, {
                    "status": "Valid",
                    "discrepancy": "Successfully inserted. No discrepancies found."
                })
                
                processed_ledgers = []
                for key in amounts:
                    if amounts[key] != 0:
                        processed_ledgers.append(f"{key}: {amounts[key]}")

                duplicate_ledgers = []
                for key in ledger_count:
                    if ledger_count[key] > 1:
                        duplicate_ledgers.append(key)

                success_msg = "Record created successfully \n"

                if processed_ledgers:
                    success_msg += "Processed: " + ", ".join(processed_ledgers)

                if duplicate_ledgers:
                    success_msg += " Duplicate ledgers set to 0: " + ", ".join(duplicate_ledgers)

                frappe.db.set_value("NPS Importer", doc.name, {
                    "remark": success_msg,
                    "status": "Success"
                })
                
            else:
                frappe.db.set_value("NPS JV Store", jv_doc.name, {
                    "status": "Discrepancy",
                    "discrepancy": f"Amount Discrepancy:\n{validation_result['difference']}"
                })

                frappe.db.set_value("NPS Importer", doc.name, {
                    "remark": "Record created successfully",
                    "status": "Success"
                })
                
        else:
            frappe.db.set_value("NPS Importer", doc.name, {
                "remark": "Record created successfully",
                "status": "Success"
            })
        
    except Exception as e:
        error_msg = str(e)
        frappe.log_error(frappe.get_traceback(), "NPS Importer Failure")
        frappe.db.set_value("NPS Importer", doc.name, {
            "remark": f"Failed: {error_msg}",
            "status": "Failed"
        })

def validate_against_database(jv_doc, file_type, file_name=None, file_content=None):
    frappe.log_error(f"Running validation for: {file_type}")
    try:
        if file_type == "Comparison JV":
            query_to = query_from = None
        else:
            query_from = jv_doc.from_date
            query_to = jv_doc.to_date
        
        if file_type == "Contribution JV":
            validation_query = """
            SELECT     
                "Date Created",     
                SUM("T1 Base Amount") AS "T1 Base Amount",     
                SUM("T1 GST") AS "T1 GST",      
                SUM("T1 Transaction Charges") AS "T1 Transaction Charges",     
                SUM("T2 Base Amount") AS "T2 Base Amount",     
                SUM("T2 GST") AS "T2 GST",     
                SUM("T2 Transaction Charges") AS "T2 Transaction Charges",      
                SUM("Registration") AS "Registration",     
                SUM("T1 Base Amount" + "T1 GST" + "T1 Transaction Charges" + "T2 Base Amount" + "T2 GST" + "T2 Transaction Charges" + "Registration") AS "Total Amount" 
            FROM (     
                SELECT         
                    (creation - INTERVAL '9 hours')::DATE AS "Date Created",         
                    COALESCE((order_details->'notes'->>'t1_amount')::NUMERIC,0) AS "T1 Base Amount",         
                    COALESCE((order_details->'notes'->>'t1_gst')::NUMERIC,0) + CASE WHEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)>0 THEN 36 ELSE 0 END AS "T1 GST",         
                    COALESCE((order_details->'notes'->>'t1_transaction_charges')::NUMERIC, 0) AS "T1 Transaction Charges",         
                    COALESCE((order_details->'notes'->>'t2_amount')::NUMERIC,0) AS "T2 Base Amount",         
                    COALESCE((order_details->'notes'->>'t2_gst')::NUMERIC,0) AS "T2 GST",         
                    COALESCE((order_details->'notes'->>'t2_transaction_charges')::NUMERIC,0) AS "T2 Transaction Charges",         
                    CASE
                        WHEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)>0
                        THEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)-36
                        ELSE 0
                    END AS "Registration"     
                FROM         
                    tabnps_contribution     
                WHERE         
                    order_details IS NOT NULL         
                    AND order_details->'notes' IS NOT NULL
                    AND status = 'captured'
                    AND (creation - INTERVAL '9 hours')::DATE BETWEEN %s AND %s
                    
                UNION ALL
                SELECT
                    (contribution_timestamp)::DATE AS "Date Created",
                    COALESCE((item->>'amount')::NUMERIC, 0) AS "T1 Base Amount",
                    (COALESCE((item->>'cgst')::NUMERIC, 0) + COALESCE((item->>'sgst')::NUMERIC, 0) + COALESCE((item->>'igst')::NUMERIC, 0)) AS "T1 GST",
                    COALESCE((item->>'service_charge')::NUMERIC, 0) AS "T1 Transaction Charges",
                    0 AS "T2 Base Amount",
                    0 AS "T2 GST",
                    0 AS "T2 Transaction Charges",
                    0 AS "Registration"
                FROM
                    tabnps_agent_contribution, 
                    jsonb_array_elements(contribution::jsonb->'items') AS item
                WHERE
                    contribution IS NOT NULL
                    AND (contribution_timestamp)::DATE = %s

            ) AS contribution_data 
            GROUP BY     
                "Date Created" 
            ORDER BY     
                "Date Created";
            """
            
            query_result = frappe.db.sql(validation_query, (query_from, query_to, query_from), as_dict=True)
            
            if not query_result:
                return {
                    "is_valid": False,
                    "difference": f"No matching data found in database for the date range {query_from} to {query_to}"
                }
            
            #db contents -> nps_contribution table
            db_total_amount = float(query_result[0].get("Total Amount", 0))
            db_order_value = float(query_result[0].get("T1 Base Amount", 0)) #T2 = 0
            db_registration_fee = float(query_result[0].get("Registration", 0))
            db_comission = float(query_result[0].get("T1 Transaction Charges", 0)) + float(query_result[0].get("Service Charges", 0)) #T2 = 0
            db_service_charges = float(query_result[0].get("Service Charges", 0))
            db_gst = float(query_result[0].get("T1 GST", 0)) #T2 = 0
            
            #jv_record contents
            jv_total_amount = float(jv_doc.total_amount or 0) 
            jv_order_value = float(jv_doc.order_value or 0)
            jv_registration_fee = float(jv_doc.registration_fee or 0)
            jv_comission = float(jv_doc.comission or 0) 
            jv_service_charges = float(jv_doc.service_charges or 0)
            jv_central_gst = float(jv_doc.central_gst or 0)
            jv_state_gst = float(jv_doc.state_gst or 0)
            jv_integrated_gst = float(jv_doc.integrated_gst or 0)
            jv_gst = jv_central_gst + jv_state_gst + jv_integrated_gst
            
            tolerance = 0.1 
            difference_total_amount = abs(db_total_amount - jv_total_amount)
            difference_order_value = abs(db_order_value - jv_order_value)
            difference_registration_fee = abs(db_registration_fee - jv_registration_fee)
            difference_comission = abs(db_comission - jv_comission)
            difference_gst = abs(db_gst-jv_gst)
            difference_service_charges = abs(db_service_charges - jv_service_charges)

            if ((difference_total_amount <= tolerance) and (difference_order_value<= tolerance) and (difference_registration_fee <= tolerance) and (difference_comission <= tolerance) and (difference_gst <= tolerance) and (difference_service_charges <= tolerance)):
                return {"is_valid": True, "difference": 0}
            
            else:
                differences = []
                if difference_total_amount>tolerance:
                    differences.append(f"DB Total: {db_total_amount}, JV Total: {jv_total_amount}, Difference in Total Amount: {difference_total_amount}")
                if difference_order_value>tolerance:
                    differences.append(f"DB Order Value: {db_order_value}, JV Order Value: {jv_order_value}, Difference in Order Value: {difference_order_value}")
                if difference_registration_fee>tolerance:
                    differences.append(f"DB Registration Fee: {db_registration_fee}, JV Registration Fee: {jv_registration_fee}, Difference in Registration Fee: {difference_registration_fee}")
                if difference_comission>tolerance:
                    differences.append(f"DB Commision Value: {db_comission}, JV Commission Value: {jv_comission}, Difference in Commission Value: {difference_comission}")
                if difference_gst>tolerance:
                    differences.append(f"DB GST Value: {db_gst}, JV GST Value: {jv_gst}, Difference in GST Value: {difference_gst}")
                if difference_service_charges>tolerance:
                    differences.append(f"DB Service Charges: {db_service_charges}, JV Service Charges: {jv_service_charges}, Difference in Service Charges: {difference_service_charges}")
                return {
                    "is_valid": False,
                    "difference": " |\n".join(differences)
                }
            
        elif file_type == "Agent Contribution JV":
            validation_query = """
            SELECT
                contribution_timestamp::DATE AS "Date Created", 
                SUM((item->>'amount')::NUMERIC) AS "Base Amount",
                SUM((item->>'cgst')::NUMERIC) AS "CGST",
                SUM((item->>'igst')::NUMERIC) AS "IGST",
                SUM((item->>'sgst')::NUMERIC) AS "SGST",
                SUM((item->>'service_charge')::NUMERIC) AS "Service Charge",
                SUM((item->>'total_amount')::NUMERIC) As "Total Amount"
            FROM
                tabnps_agent_contribution,
                jsonb_array_elements(contribution::jsonb->'items') AS item  
            WHERE
                (contribution_timestamp)::DATE = %s
            GROUP BY
                "Date Created"
            ORDER BY 
                "Date Created"
            """

            query_result = frappe.db.sql(validation_query, (query_from,), as_dict=True)

            if not query_result:
                return{
                    "is_valid": False,
                    "difference": "No matching data found in the database for this date"
                }
            
            db_total_amount = float(query_result[0].get("Total Amount", 0))
            db_order_value = float(query_result[0].get("Base Amount", 0))
            db_service_charges = float(query_result[0].get("Service Charge", 0))
            db_cgst = float(query_result[0].get("CGST", 0))
            db_sgst = float(query_result[0].get("SGST", 0))
            db_igst = float(query_result[0].get("IGST", 0))
            db_gst = db_cgst + db_sgst + db_igst

            jv_total_amount = float(jv_doc.total_amount or 0)
            jv_order_value = float(jv_doc.order_value or 0)
            jv_service_charges = float(jv_doc.service_charges or 0)
            jv_cgst = float(jv_doc.central_gst or 0)
            jv_sgst = float(jv_doc.state_gst or 0)
            jv_igst = float(jv_doc.integrated_gst or 0)
            jv_gst = jv_cgst + jv_sgst + jv_igst

            tolerance = 0.01
            difference_total_amount = abs(db_total_amount - jv_total_amount)
            difference_order_value = abs(db_order_value - jv_order_value)
            difference_service_charges = abs(db_service_charges - jv_service_charges)
            difference_gst = abs(db_gst - jv_gst)

            if ((difference_total_amount <= tolerance) and (difference_order_value <= tolerance) and (difference_service_charges <= tolerance) and (difference_gst <= tolerance)):
                return{
                    "is_valid": True,
                    "difference": 0
                }
            
            else:
                differences = []
                if difference_total_amount > tolerance:
                    differences.append(f"DB Total: {db_total_amount}, JV Total: {jv_total_amount}, Difference in Total Amount: {difference_total_amount}")
                if difference_order_value > tolerance:
                    differences.append(f"DB Order Value: {db_order_value}, JV Order Value: {jv_order_value}, Difference in Order Value: {difference_order_value}")
                if difference_service_charges > tolerance:
                    differences.append(f"DB Service Charge: {db_service_charges}, JV Service Charges: {jv_service_charges}, Difference in Service Charge: {difference_service_charges}")
                if difference_gst > tolerance:
                    differences.append(f"DB GST: {db_gst}, JV GST: {jv_gst}, Difference in GST Value: {difference_gst}")
                return{
                    "is_valid": False,
                    "difference": "|\n".join(differences)
                }
            
        elif file_type == "Modification JV":
            validation_query = """
            SELECT 
                SUM(amount) AS "Total Amount"
            FROM
                "tabNPS Charge"
            WHERE 
                creation::DATE = %s
                AND status = 'captured'
            """

            query_result = frappe.db.sql(validation_query, (query_from,), as_dict=True)

            if not query_result:
                return{
                    "is_valid": False,
                    "difference": "No matching data found in NPS Charge table for this data"
                }
            
            db_total_amount = float(query_result[0].get("Total Amount", 0) or 0)
            jv_total_amount = float(jv_doc.total_amount or 0)

            tolerance = 0.01
            difference_total_amount = abs(db_total_amount - jv_total_amount)

            if difference_total_amount <= tolerance:
                return{
                    "is_valid": True,
                    "difference": 0
                }
            else:
                return{
                    "is_valid": False,
                    "difference": f"DB Total Amount:{db_total_amount}, JV Total Amount:{jv_total_amount}, Difference:{difference_total_amount}"
                }            

        elif file_type == "Comparison JV":
            try:
                csv_rows = []
                f = io.StringIO(file_content)  
                reader = csv.reader(f)
                csv_rows = list(reader)
                
                if len(csv_rows) <= 1:
                    return{
                        "is_valid": False,
                        "difference": "No data rows found in CSV file"
                    }

                is_nps_agent_transaction = 'agent' in file_name
                is_nps_transaction = not is_nps_agent_transaction

                if not is_nps_transaction and not is_nps_agent_transaction:
                    return{
                        "is_valid": False,
                        "difference": f"Unable to determine transaction type from filename: {file_name}"
                    }

                total_discrepancies = 0
                combined_remarks = []
                # transaction_file_path = '/tmp/transactions.csv'
                # agent_transaction_file_path = '/tmp/transactions_agent.csv'

                if is_nps_transaction:
                    create_temp_table1 = """
                    CREATE TEMP TABLE "tabNPS Transaction"(
                    entity_id varchar(50), 
                    type varchar(30), 
                    debit float, 
                    credit float, 
                    amount float, 
                    currency varchar(10), 
                    fee float, 
                    tax float, 
                    on_hold int, 
                    settled int, 
                    created_at timestamp, 
                    settled_at date, 
                    settlement_id varchar(50), 
                    description varchar(100), 
                    notes json, 
                    payment_id varchar(50), 
                    arn varchar(50), 
                    settlement_utr varchar(50), 
                    order_id varchar(50), 
                    order_receipt varchar(50), 
                    method varchar(30), 
                    upi_flow varchar(50), 
                    card_network varchar(50), 
                    card_issuer varchar(50), 
                    card_type varchar(50), 
                    dispute_id varchar(50), 
                    additional_utr varchar(50)
                    );"""
                    frappe.db.sql(create_temp_table1)

                    alter_table1_notnull = """
                    ALTER TABLE "tabNPS Transaction" 
                    ALTER COLUMN entity_id SET NOT NULL, 
                    ALTER COLUMN type SET NOT NULL, 
                    ALTER COLUMN debit SET NOT NULL, 
                    ALTER COLUMN credit SET NOT NULL, 
                    ALTER COLUMN amount SET NOT NULL, 
                    ALTER COLUMN currency SET NOT NULL, 
                    ALTER COLUMN fee SET NOT NULL, 
                    ALTER COLUMN tax SET NOT NULL, 
                    ALTER COLUMN on_hold SET NOT NULL, 
                    ALTER COLUMN settled SET NOT NULL, 
                    ALTER COLUMN created_at SET NOT NULL, 
                    ALTER COLUMN settled_at SET NOT NULL;
                    """
                    frappe.db.sql(alter_table1_notnull)

                    alter_table1_contraints = """
                    ALTER TABLE "tabNPS Transaction" 
                    ADD CONSTRAINT entity_id_unique UNIQUE(entity_id);
                    """
                    frappe.db.sql(alter_table1_contraints)

                    # to fix date format of the form dd/mm/yy in order to copy from csv(change to text field)
                    alter_table1_date_type = """ 
                    ALTER TABLE "tabNPS Transaction"
                    ALTER COLUMN created_at TYPE TEXT, 
                    ALTER COLUMN settled_at TYPE TEXT
                    """
                    frappe.db.sql(alter_table1_date_type)

                    # copy_table1 = f"""
                    # COPY "tabNPS Transaction" FROM '{temp_file_path}' DELIMITER ',' CSV HEADER;
                    # """
                    # frappe.db.sql(copy_table1)

                    insert_query = """
                    INSERT INTO "tabNPS Transaction" (
                        entity_id, type, debit, credit, amount, currency, fee, tax, 
                        on_hold, settled, created_at, settled_at, settlement_id, description, 
                        notes, payment_id, arn, settlement_utr, order_id, order_receipt, 
                        method, upi_flow, card_network, card_issuer, card_type, dispute_id, additional_utr
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    
                    for row in csv_rows[1:]:
                        if len(row) >= 26:  
                            try:                                
                                debit = float(row[2]) if row[2] else 0.0
                                credit = float(row[3]) if row[3] else 0.0
                                amount = float(row[4]) if row[4] else 0.0
                                fee = float(row[6]) if row[6] else 0.0
                                tax = float(row[7]) if row[7] else 0.0
                                on_hold = int(row[8]) if row[8] else 0
                                settled = int(row[9]) if row[9] else 0
                                
                                notes = row[14] if row[14] else None  #notes 

                                if not row[18] or row[18] == "":  #skip if order_id is null
                                    continue
                                
                                frappe.db.sql(insert_query, (
                                    row[0],  # entity_id
                                    row[1],  # type
                                    debit,   # debit
                                    credit,  # credit
                                    amount,  # amount
                                    row[5],  # currency
                                    fee,     # fee
                                    tax,     # tax
                                    on_hold, # on_hold
                                    settled, # settled
                                    row[10],  # created_at
                                    row[11] if len(row)>11 else None, # settled_at
                                    row[12] if len(row)>12 else None, # settlement_id
                                    row[13] if len(row)>13 else None, # description
                                    notes,   # notes
                                    row[15] if len(row) > 15 else None, # payment_id
                                    row[16] if len(row) > 16 else None, # arn
                                    row[17] if len(row) > 17 else None, # settlement_utr
                                    row[18],
                                    row[19] if len(row) > 19 else None, # order_receipt
                                    row[20] if len(row) > 20 else None, # method
                                    row[21] if len(row) > 21 else None, # upi_flow
                                    row[22] if len(row) > 22 else None, # card_network
                                    row[23] if len(row) > 23 else None, # card_issuer
                                    row[24] if len(row) > 24 else None, # card_type
                                    row[25] if len(row) > 25 else None, # dispute_id
                                    row[26] if len(row) > 26 else None  # additional_utr
                                ))
                            except (ValueError, IndexError) as e:
                                continue

                    #test query
                    # count_query1 = 'SELECT COUNT(*) as count FROM "tabNPS Transaction"'
                    # count1 = frappe.db.sql(count_query1, as_dict=True)
                    # frappe.log_error(f"Rows inserted into tabNPS Transaction: {count1[0]['count']}")

                    #convert date column back to date type after copying
                    convert_table1_date = """
                    ALTER TABLE "tabNPS Transaction"
                    ALTER COLUMN created_at TYPE TIMESTAMP USING TO_TIMESTAMP(created_at, 'DD/MM/YYYY HH24:MI:SS'),
                    ALTER COLUMN settled_at TYPE DATE USING TO_DATE(settled_at, 'DD-MM-YYYY');
                    """
                    frappe.db.sql(convert_table1_date)

                    comparison_query_1 = """
                    SELECT t.order_id 
                    FROM "tabNPS Transaction" t
                    LEFT JOIN tabnps_contribution c
                    ON t.order_id = c.order_id
                    WHERE t.order_id IS NOT NULL
                    AND t.settled_at IS NOT NULL
                    AND t.description IS NOT NULL
                    AND c.order_id IS NULL;
                    """
                    missing_nps_orders = frappe.db.sql(comparison_query_1, as_dict=True)
                    missing_nps_count = len(missing_nps_orders)

                    transaction_remarks = []
                    transaction_remarks.append(f"NPS Discrepancies found: {missing_nps_count}")

                    if missing_nps_count:
                        nps_order_ids = [row['order_id'] for row in missing_nps_orders]
                        transaction_remarks.append("Missing NPS Order IDs:")
                        transaction_remarks.append(", ".join(nps_order_ids))
                    else:
                        transaction_remarks.append("No missing NPS Orders")
                    transaction_remarks.append("")

                    frappe.db.sql('DROP TABLE IF EXISTS "tabNPS Transaction"')
                    combined_remarks.extend(transaction_remarks)
                    total_discrepancies += missing_nps_count
                    
                elif is_nps_agent_transaction:
                    create_temp_table2 = """
                    CREATE TEMP TABLE "tabNPS Agent Transaction"(
                    entity_id varchar(50), 
                    type varchar(50), 
                    debit float, 
                    credit float, 
                    amount float, 
                    currency varchar(3), 
                    fee float, 
                    tax float, 
                    on_hold int, 
                    settled int, 
                    created_at timestamp, 
                    settled_at date, 
                    settlement_id varchar(50), 
                    description varchar(300), 
                    notes json, 
                    payment_id varchar(50), 
                    arn varchar(50), 
                    settlement_utr varchar(50), 
                    order_id varchar(50), 
                    order_receipt varchar(50), 
                    method varchar(50), 
                    upi_flow varchar(50), 
                    card_network varchar(50), 
                    card_issuer varchar(50), 
                    card_type varchar(50), 
                    dispute_id varchar(50), 
                    additional_utr varchar(50)
                    );"""
                    frappe.db.sql(create_temp_table2)

                    alter_table2_notnull = """
                    ALTER TABLE "tabNPS Agent Transaction" 
                    ALTER COLUMN entity_id SET NOT NULL, 
                    ALTER COLUMN type SET NOT NULL, 
                    ALTER COLUMN debit SET NOT NULL, 
                    ALTER COLUMN credit SET NOT NULL, 
                    ALTER COLUMN amount SET NOT NULL, 
                    ALTER COLUMN currency SET NOT NULL, 
                    ALTER COLUMN fee SET NOT NULL, 
                    ALTER COLUMN tax SET NOT NULL, 
                    ALTER COLUMN on_hold SET NOT NULL, 
                    ALTER COLUMN settled SET NOT NULL, 
                    ALTER COLUMN created_at SET NOT NULL;
                    """
                    frappe.db.sql(alter_table2_notnull)

                    alter_table2_constraints = """
                    ALTER TABLE "tabNPS Agent Transaction" 
                    ADD CONSTRAINT entity_id_unique2 UNIQUE(entity_id);
                    """
                    frappe.db.sql(alter_table2_constraints)

                    alter_table2_date_type = """
                    ALTER TABLE "tabNPS Agent Transaction"
                    ALTER COLUMN created_at TYPE TEXT,
                    ALTER COLUMN settled_at TYPE TEXT
                    """
                    frappe.db.sql(alter_table2_date_type)

                    insert_query = """
                    INSERT INTO "tabNPS Agent Transaction" (
                        entity_id, type, debit, credit, amount, currency, fee, tax, 
                        on_hold, settled, created_at, settled_at, settlement_id, description, 
                        notes, payment_id, arn, settlement_utr, order_id, order_receipt, 
                        method, upi_flow, card_network, card_issuer, card_type, dispute_id, additional_utr
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    for row in csv_rows[1:]:
                        if len(row) >= 15:  
                            try:                                
                                debit = float(row[2]) if row[2] else 0.0
                                credit = float(row[3]) if row[3] else 0.0
                                amount = float(row[4]) if row[4] else 0.0
                                fee = float(row[6]) if row[6] else 0.0
                                tax = float(row[7]) if row[7] else 0.0
                                on_hold = int(row[8]) if row[8] else 0
                                settled = int(row[9]) if row[9] else 0
                            
                                notes = row[12] if len(row) > 12 and row[12] else None #notes
                                
                                frappe.db.sql(insert_query, (
                                    row[0],  # entity_id
                                    row[1],  # type
                                    debit,   # debit
                                    credit,  # credit
                                    amount,  # amount
                                    row[5],  # currency
                                    fee,     # fee
                                    tax,     # tax
                                    on_hold, # on_hold
                                    settled, # settled
                                    row[10],  # created_at
                                    row[11] if len(row) > 11 else None,  # settled_at
                                    row[11] if len(row) > 11 else None, # settlement_id (not in agent sample)
                                    row[11] if len(row) > 11 else None, # description (recurring payment info)
                                    notes,   # notes
                                    row[13] if len(row) > 13 else None, # payment_id (not in agent sample)
                                    row[14] if len(row) > 14 else None, # arn (not in agent sample)
                                    row[15] if len(row) > 15 else None, # settlement_utr (not in agent sample)
                                    row[13],
                                    row[14] if len(row) > 14 else None, # order_receipt
                                    row[15] if len(row) > 15 else None, # method
                                    row[16] if len(row) > 16 else None, # upi_flow
                                    row[17] if len(row) > 17 else None, # card_network
                                    row[18] if len(row) > 18 else None, # card_issuer
                                    row[19] if len(row) > 19 else None, # card_type
                                    row[20] if len(row) > 20 else None, # dispute_id
                                    row[21] if len(row) > 21 else None  # additional_utr
                                ))
                            except (ValueError, IndexError) as e:
                                # Skip invalid rows
                                continue

                    # copy_table2 = f"""
                    # COPY "tabNPS Agent Transaction" FROM '{temp_file_path}' DELIMITER ',' CSV HEADER;
                    # """
                    # frappe.db.sql(copy_table2)

                    # count_query2 = 'SELECT COUNT(*) as count FROM "tabNPS Agent Transaction"'
                    # count2 = frappe.db.sql(count_query2, as_dict=True)
                    # frappe.log_error(f"Rows inserted into tabNPS Agent Transaction: {count2[0]['count']}")

                    convert_table2_date = """
                    ALTER TABLE "tabNPS Agent Transaction"
                    ALTER COLUMN created_at TYPE TIMESTAMP USING TO_TIMESTAMP(created_at, 'DD/MM/YYYY HH24:MI:SS'),
                    ALTER COLUMN settled_at TYPE DATE USING TO_DATE(settled_at, 'DD-MM-YYYY');
                    """
                    frappe.db.sql(convert_table2_date)

                    comparison_query_2 = """
                    SELECT t.order_id
                    FROM "tabNPS Agent Transaction" t
                    LEFT JOIN tabnps_agent_contribution c
                    ON t.entity_id = (c.payment_aggregator_meta->>'reference_id')
                    WHERE t.entity_id LIKE 'pay_%'
                    AND t.order_id IS NOT NULL
                    AND t.order_receipt IS NOT NULL
                    AND (c.payment_aggregator_meta->>'reference_id') IS NULL;
                    """
                    missing_agent_orders = frappe.db.sql(comparison_query_2, as_dict=True)
                    missing_agent_count = len(missing_agent_orders)

                    agent_remarks = []
                    agent_remarks.append(f"Discrepancies found: {missing_agent_count}")
                    if missing_agent_count:
                        agent_order_ids = [row['order_id'] for row in missing_agent_orders]
                        agent_remarks.append("Missing Agent Order IDs:")
                        agent_remarks.append(", ".join(agent_order_ids))
                    else:
                        agent_remarks.append("No discrepancies found.")
                    agent_remarks.append("")

                    frappe.db.sql('DROP TABLE IF EXISTS "tabNPS Agent Transaction"')
                    combined_remarks.extend(agent_remarks)
                    total_discrepancies += missing_agent_count

                combined_remarks.append(f"Total Discrepancies: {total_discrepancies}")

                return{
                    "is_valid": total_discrepancies == 0,
                    "difference": "\n".join(combined_remarks),
                    "total_discrepancies": total_discrepancies
                }

            except Exception as comparison_error:
                frappe.log_error(f"Comparison JV error: {str(comparison_error)}", "Comparison JV Validation Error")
                return {
                    "is_valid": False,
                    "difference": f"Comparison validation error: {str(comparison_error)}"
                }

        else:
            return {
                "is_valid": False,
                "difference": f"Unknown File Type: {file_type}"
            }
                
    except Exception as e:
        frappe.log_error(f"Validation error: {str(e)}", "NPS Validation Error")
        return {
            "is_valid": False,
            "difference": f"Validation error: {str(e)}"
        }