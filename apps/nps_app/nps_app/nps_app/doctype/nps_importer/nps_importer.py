import frappe
import csv
import io
import frappe.utils
import os

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
            frappe.log_error("Entered Comparison JV")
            validation_result = validate_against_database(None, doc.file_type)

            if validation_result["is_valid"]:
                comp_doc = frappe.new_doc("NPS Transactions")
                comp_doc.discrepancy_count = 0
                comp_doc.remark = "No discrepancies found."
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

                remarks = []
                total_discrepancies = validation_result.get('total_discrepancies', 0)

                remarks.append(f"Total Discrepancies Found: {total_discrepancies}")

                if validation_result.get('missing_nps_orders'):
                    nps_order_ids = [order.get('order_id', '') for order in validation_result['missing_nps_orders']]
                    remarks.append(f"Missing NPS Orders: ({len(nps_order_ids)}):")
                    remarks.append(', '.join(nps_order_ids))
                    remarks.append("")

                if validation_result.get('missing_agent_orders'):
                    agent_order_ids = [order.get('order_id', '') for order in validation_result['missing_agent_orders']]
                    remarks.append(f"Missing Agent Orders: ({len(agent_order_ids)}):")
                    remarks.append(', '.join(agent_order_ids))
                    remarks.append("")

                full_diff = validation_result.get('difference', [])
                if full_diff:
                    remarks.append("Discrepancies Found:")
                    if isinstance(full_diff, list):
                        remarks.extend(full_diff)
                    elif isinstance(full_diff, str):
                        remarks.extend(full_diff)

                final_remark = "\n".join(remarks) if remarks else "Discrepancies found but no details"
            
                comp_doc.remark = final_remark
                comp_doc.insert(ignore_permissions=True, ignore_mandatory=True)

                frappe.db.set_value("NPS Importer", doc.name, {
                    "status": "Success",
                    "remark": "Discrepancy log saved."
                })
            frappe.log_error(f"Comparison JV completed. NPS Transactions document created:{comp_doc.name}")
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

def validate_against_database(jv_doc, file_type):
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
            frappe.log_error("Entered Comparison JV block")
            try:
                transaction_file_path = '/tmp/transactions.csv'
                agent_transaction_file_path = '/tmp/transactions-2.csv'

                frappe.log_error(f"transactions.csv exists: {os.path.exists(transaction_file_path)}")
                frappe.log_error(f"transactions-2.csv exists: {os.path.exists(agent_transaction_file_path)}")

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
                frappe.log_error("Created NPS Transactions Table")

                create_temp_table2 = """
                CREATE TEMP TABLE "tabNPS Agent Transaction"(
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
                frappe.db.sql(create_temp_table2)
                frappe.log_error("Created Agent Transactions Table")

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
                frappe.log_error("Altering table1 columns - NOT NULL property")

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
                frappe.log_error("Altering table2 columns - NOT NULL property")

                alter_table1_contraints = """
                ALTER TABLE "tabNPS Transaction" 
                ADD CONSTRAINT entity_id_unique UNIQUE(entity_id), 
                ADD CONSTRAINT order_id_unique UNIQUE(order_id), 
                ADD CONSTRAINT order_receipt_unique UNIQUE(order_receipt);
                """
                frappe.db.sql(alter_table1_contraints)
                frappe.log_error("Altering table1 constraints - UNIQUE")

                alter_table2_constraints = """
                ALTER TABLE "tabNPS Agent Transaction" 
                ADD CONSTRAINT entity_id_unique2 UNIQUE(entity_id);
                """
                frappe.db.sql(alter_table2_constraints)
                frappe.log_error("Altering table 2 constraints - UNIQUE")

                # to fix date format of the form dd/mm/yy in order to copy from csv(change to text field)
                alter_table1_date_type = """ 
                ALTER TABLE "tabNPS Transaction"
                ALTER COLUMN created_at TYPE TEXT, 
                ALTER COLUMN settled_at TYPE TEXT
                """
                frappe.db.sql(alter_table1_date_type)
                frappe.log_error("Altering table1 date format to text to copy")

                alter_table2_date_type = """
                ALTER TABLE "tabNPS Agent Transaction"
                ALTER COLUMN created_at TYPE TEXT,
                ALTER COLUMN settled_at TYPE TEXT
                """
                frappe.db.sql(alter_table2_date_type)
                frappe.log_error("Altering table2 date format to text to copy")

                copy_table1 = f"""
                COPY "tabNPS Transaction" FROM '{transaction_file_path}' DELIMITER ',' CSV HEADER;
                """
                frappe.db.sql(copy_table1)
                frappe.log_error("Copying contents to table1")

                count_query1 = 'SELECT COUNT(*) as count FROM "tabNPS Transaction"'
                count1 = frappe.db.sql(count_query1, as_dict=True)
                frappe.log_error(f"Rows inserted into tabNPS Transaction: {count1[0]['count']}")

                copy_table2 = f"""
                COPY "tabNPS Agent Transaction" FROM '{agent_transaction_file_path}' DELIMITER ',' CSV HEADER;
                """
                frappe.db.sql(copy_table2)
                frappe.log_error("Copying contents to table2")

                count_query2 = 'SELECT COUNT(*) as count FROM "tabNPS Agent Transaction"'
                count2 = frappe.db.sql(count_query2, as_dict=True)
                frappe.log_error(f"Rows inserted into tabNPS Agent Transaction: {count2[0]['count']}")

                #convert date column back to date type after copying
                convert_table1_date = """
                ALTER TABLE "tabNPS Transaction"
                ALTER COLUMN created_at TYPE TIMESTAMP USING TO_TIMESTAMP(created_at, 'DD/MM/YYYY HH24:MI:SS'),
                ALTER COLUMN settled_at TYPE DATE USING TO_DATE(settled_at, 'DD-MM-YYYY');
                """
                frappe.db.sql(convert_table1_date)
                frappe.log_error("Converting date format back to date in table1")

                convert_table2_date = """
                ALTER TABLE "tabNPS Agent Transaction"
                ALTER COLUMN created_at TYPE TIMESTAMP USING TO_TIMESTAMP(created_at, 'DD/MM/YYYY HH24:MI:SS'),
                ALTER COLUMN settled_at TYPE DATE USING TO_DATE(settled_at, 'DD-MM-YYYY');
                """
                frappe.db.sql(convert_table2_date)
                frappe.log_error("Converting date format back to date in table2")

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
                frappe.log_error("Comparison query on table1")

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
                frappe.log_error("Comparison query on table2")

                missing_nps_count = len(missing_nps_orders)
                missing_agent_count = len(missing_agent_orders)

                combined_results = {
                    'missing_nps_orders': missing_nps_orders,
                    'missing_agent_orders': missing_agent_orders,
                    'nps_count': missing_nps_count,
                    'agent_count': missing_agent_count
                }
        
                frappe.db.sql('DROP TABLE IF EXISTS "tabNPS Transaction"')
                frappe.db.sql('DROP TABLE IF EXISTS "tabNPS Agent Transaction"')
        
                total_discrepancies = missing_nps_count + missing_agent_count

                discrepancy_details = []
                discrepancy_details.append(f"Total discrepancies: {total_discrepancies}\n")
                discrepancy_details.append(f"Missing NPS Orders Count: {missing_nps_count}\n")
                discrepancy_details.append(f"Missing NPS Orders: {missing_nps_orders}\n")
                discrepancy_details.append(f"Missing Agent Orders Count: {missing_agent_count}\n")
                discrepancy_details.append(f"Missing Agent Orders: {missing_agent_orders}\n")

                if missing_nps_count>0:
                    nps_order_ids = [order.get('order_id', '') for order in combined_results['missing_nps_orders']]
                    discrepancy_details.append(f"NPS Orders not found in contributions: {', '.join(nps_order_ids)}")
                else:
                    discrepancy_details.append("All NPS records match.")

                if missing_agent_count>0:
                    agent_order_ids = [order.get('order_id', '') for order in combined_results['missing_agent_orders']]
                    discrepancy_details.append(f"Agent Orders not found in contributions: {', '.join(agent_order_ids)}")
                else:
                    discrepancy_details.append("All agent records match.")

                if total_discrepancies == 0:
                    return {
                        "is_valid": True,
                        "difference": discrepancy_details,
                        "total_discrepancies": 0,
                        "missing_nps_orders": [],
                        "missing_agent_orders": []
                    }
                else:
                    return {
                        "is_valid": False,
                        "difference": discrepancy_details,
                        "total_discrepancies": total_discrepancies,
                        "missing_nps_orders": missing_nps_orders,
                        "missing_agent_orders": missing_agent_orders
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