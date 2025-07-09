import io
import os
import csv
import frappe

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

tolerance = 0.01 

def process_file(doc, method=None):
    if doc.status != "pending":
        raise Exception("Only pending imports can be processed.")
    
    if not doc.uploaded_file:
        raise Exception("No supporting file found.")
    
    frappe.db.begin() #start transaction
    frappe.db.set_value("NPS Importer", doc.name, "status", "processing") #set status to processing
    
    try:
        if doc.file_type == "Comparison":
            response = validate_against_database(doc.file_type, doc=doc) 

            if response["status"].lower() == "error":
                frappe.db.set_value("NPS Importer", doc.name, {
                    "remark": response["remark"],
                    "status": "failed"
                })

            else:
                comp_doc = frappe.new_doc("NPS Transactions")
                comp_doc.discrepancy_count = response["record_count"]
                comp_doc.remarks = response["remark"]
                comp_doc.insert(ignore_permissions=True, ignore_mandatory=True)

                frappe.db.set_value("NPS Importer", doc.name ,{
                    "remark": "Discrepancy log saved." if comp_doc.discrepancy_count else "Comparison completed successfully.",
                    "status": "success"
                })

        else:
            rows = None
            file_doc = frappe.get_doc("File", {"file_url": doc.uploaded_file})
            file_content = file_doc.get_content()
                
            f = io.StringIO(file_content)
            reader = csv.reader(f, delimiter=',')
            rows = list(reader) #content of csv

            if len(rows) > 1 and len(rows[0]) >= 10:
                rows = rows
            else:
                frappe.db.set_value("NPS Importer", doc.name, {
                    "remark": "No valid data rows found in file",
                    "status": "failed"
                })
                
            amounts = {}
            tmp_ledger_pool = []
            
            for idx, row in enumerate(rows):
                #date_val = row[3].strip()
                ledger_code = row[5].strip()
                amount = float(row[8].strip())

                if ledger_code in ledger_mapping:
                    if ledger_code in tmp_ledger_pool:
                        raise Exception(f"Invalid file content. Ledger code {ledger_code} repeats in the file.")
                    
                    tmp_ledger_pool.append(ledger_code)
                    amounts[ledger_mapping[ledger_code]] = amount

            if amounts.get("total_amount", 0) < 1:
                raise Exception("Invalid file content. Missing `total_amount`.")
            
            jv_doc = frappe.new_doc("NPS JV Store")
            
            jv_doc.from_date = doc.from_date
            jv_doc.to_date = doc.to_date
            jv_doc.user_id = row[10].strip()
            jv_doc.status = "pending"  # set to pending during processing
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

            validation_result = validate_against_database(doc.file_type, doc=jv_doc)
            
            if (validation_result["status"]=="valid"):
                frappe.db.set_value("NPS JV Store", jv_doc.name, {
                    "status": validation_result["status"],
                    "remark": validation_result["remark"]
                })

            elif (validation_result["status"]=="error"):
                frappe.db.set_value("NPS JV Store", jv_doc.name, {
                    "status": validation_result["status"],
                    "remark": f"Error during processing: {validation_result['remark']}"         
                })
                
            elif (validation_result["status"]=="discrepancy"):
                frappe.db.set_value("NPS JV Store", jv_doc.name, {
                    "status": validation_result["status"],
                    "discrepancy": f"Amount Discrepancy:\n{validation_result['remark']}"
                })

            frappe.db.set_value("NPS Importer", doc.name, {
                "remark": f"Record created successfully, {validation_result['status']} please review.",
                "status": "Success"
            })

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "NPS Importer Failure")
        frappe.db.set_value("NPS Importer", doc.name, {
            "remark": f"{str(e)}",
            "status": "Failed"
        })

    finally:
        frappe.db.commit() #end transaction

def validate_against_database(file_type, doc=None, filepath=None, file_content=None):
    frappe.log_error(f"Running validation for: {file_type}")
    try:
        if file_type == "Contribution JV":
            validation_query = """
            SELECT SUM(t1_base_amount) AS t1_base_amount,     
                SUM(t1_gst) AS t1_gst,      
                SUM(t1_transaction_charges) AS t1_transaction_charges,     
                SUM(t2_base_amount) AS t2_base_amount,     
                SUM(t2_gst) AS t2_gst,     
                SUM(t2_transaction_Charges) AS t2_transaction_charges,      
                SUM(registration) AS registration,     
                SUM(t1_base_amount + t1_gst + t1_transaction_charges + t2_base_amount + t2_gst + t2_transaction_charges + registration) AS total_amount 
            FROM (     
                SELECT (creation - INTERVAL '9 hours')::DATE AS created_date,         
                    COALESCE((order_details->'notes'->>'t1_amount')::NUMERIC,0) AS t1_base_amount,         
                    COALESCE((order_details->'notes'->>'t1_gst')::NUMERIC,0) + CASE WHEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)>0 THEN 36 ELSE 0 END AS t1_gst,         
                    COALESCE((order_details->'notes'->>'t1_transaction_charges')::NUMERIC, 0) AS t1_transaction_charges,         
                    COALESCE((order_details->'notes'->>'t2_amount')::NUMERIC,0) AS t2_base_amount,         
                    COALESCE((order_details->'notes'->>'t2_gst')::NUMERIC,0) AS t2_gst,         
                    COALESCE((order_details->'notes'->>'t2_transaction_charges')::NUMERIC,0) AS t2_transaction_charges,         
                    CASE
                        WHEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)>0
                        THEN COALESCE((order_details->'notes'->>'registration')::NUMERIC, 0)-36
                        ELSE 0
                    END AS registration     
                FROM tabnps_contribution     
                WHERE order_details IS NOT NULL         
                    AND order_details->'notes' IS NOT NULL
                    AND status = 'captured'
                    AND (creation - INTERVAL '9 hours')::DATE BETWEEN %s AND %s
                UNION ALL
                SELECT (contribution_timestamp)::DATE AS created_date,
                    COALESCE((item->>'amount')::NUMERIC, 0) AS t1_base_amount,
                    (COALESCE((item->>'cgst')::NUMERIC, 0) + COALESCE((item->>'sgst')::NUMERIC, 0) + COALESCE((item->>'igst')::NUMERIC, 0)) AS t1_gst,
                    COALESCE((item->>'service_charge')::NUMERIC, 0) AS t1_transaction_charges,
                    0 AS t2_base_amount,
                    0 AS t2_gst,
                    0 AS t2_transaction_charges,
                    0 AS registration
                FROM tabnps_agent_contribution, 
                    jsonb_array_elements(contribution::jsonb->'items') AS item
                WHERE contribution IS NOT NULL
                    AND (contribution_timestamp)::DATE = %s
            ) AS contribution_data;
            """
            
            query_result = frappe.db.sql(validation_query, (doc.from_date, doc.to_date, doc.from_date), as_dict=True) #to deal with weekend case
            
            if not query_result:
                return {
                    "status": "error",
                    "remark": f"No matching data found in database for the date range {doc.from_date} to {doc.to_date}"
                }

            db_total_amount = float(query_result[0].get("total_amount", 0))
            db_order_value = float(query_result[0].get("t1_base_amount", 0)) #T2 = 0
            db_registration_fee = float(query_result[0].get("registration", 0))
            db_comission = float(query_result[0].get("t1_transaction_charges", 0)) #T2 = 0
            db_service_charges = float(query_result[0].get("service_charges", 0))
            db_gst = float(query_result[0].get("t1_gst", 0)) #T2 = 0
            
            #jv_record contents
            jv_total_amount = float(doc.total_amount or 0) 
            jv_order_value = float(doc.order_value or 0)
            jv_registration_fee = float(doc.registration_fee or 0)
            jv_comission = float(doc.comission or 0) 
            jv_service_charges = float(doc.service_charges or 0)
            jv_central_gst = float(doc.central_gst or 0)
            jv_state_gst = float(doc.state_gst or 0)
            jv_integrated_gst = float(doc.integrated_gst or 0)
            jv_gst = jv_central_gst + jv_state_gst + jv_integrated_gst
            
            difference_total_amount = abs(db_total_amount - jv_total_amount)
            difference_order_value = abs(db_order_value - jv_order_value)
            difference_registration_fee = abs(db_registration_fee - jv_registration_fee)
            difference_comission = abs(db_comission - jv_comission)
            difference_gst = abs(db_gst-jv_gst)
            difference_service_charges = abs(db_service_charges - jv_service_charges)

            status = "discrepancy"

            differences = []
            if difference_total_amount > tolerance:
                differences.append(f"DB Total: {db_total_amount}, JV Total: {jv_total_amount}, Difference in Total Amount: {difference_total_amount}.")
            
            if difference_order_value > tolerance:
                differences.append(f"DB Order Value: {db_order_value}, JV Order Value: {jv_order_value}, Difference in Order Value: {difference_order_value}.")
            
            if difference_registration_fee > tolerance:
                differences.append(f"DB Registration Fee: {db_registration_fee}, JV Registration Fee: {jv_registration_fee}, Difference in Registration Fee: {difference_registration_fee}.")
            
            if difference_comission > tolerance:
                differences.append(f"DB Commision Value: {db_comission}, JV Commission Value: {jv_comission}, Difference in Commission Value: {difference_comission}.")
            
            if difference_gst > tolerance:
                differences.append(f"DB GST Value: {db_gst}, JV GST Value: {jv_gst}, Difference in GST Value: {difference_gst}.")
            
            if difference_service_charges > tolerance:
                differences.append(f"DB Service Charges: {db_service_charges}, JV Service Charges: {jv_service_charges}, Difference in Service Charges: {difference_service_charges}.")
            
            if len(differences) == 0:
                status = "valid"
 
            return {
                "status": status,
                "remark": " ".join(differences)
            }
            
        elif file_type == "Agent Contribution JV":
            validation_query = """
            SELECT SUM((item->>'amount')::NUMERIC) AS base_amount,
                SUM((item->>'cgst')::NUMERIC) AS cgst,
                SUM((item->>'igst')::NUMERIC) AS igst,
                SUM((item->>'sgst')::NUMERIC) AS sgst,
                SUM((item->>'service_charge')::NUMERIC) AS service_charge,
                SUM((item->>'total_amount')::NUMERIC) As total_amount
            FROM
                tabnps_agent_contribution,
                jsonb_array_elements(contribution::jsonb->'items') AS item  
            WHERE
                (contribution_timestamp)::DATE = %s
            """

            query_result = frappe.db.sql(validation_query, (doc.from_date,), as_dict=True)

            if not query_result:
                return{
                    "status": "error",
                    "remark": "No matching data found in the database for this date"
                }
            
            db_total_amount = float(query_result[0].get("total_amount", 0))
            db_order_value = float(query_result[0].get("base_amount", 0))
            db_service_charges = float(query_result[0].get("service_charge", 0))
            db_cgst = float(query_result[0].get("cgst", 0))
            db_sgst = float(query_result[0].get("sgst", 0))
            db_igst = float(query_result[0].get("igst", 0))
            db_gst = db_cgst + db_sgst + db_igst

            jv_total_amount = float(doc.total_amount or 0)
            jv_order_value = float(doc.order_value or 0)
            jv_service_charges = float(doc.service_charges or 0)
            jv_cgst = float(doc.central_gst or 0)
            jv_sgst = float(doc.state_gst or 0)
            jv_igst = float(doc.integrated_gst or 0)
            jv_gst = jv_cgst + jv_sgst + jv_igst

            difference_total_amount = abs(db_total_amount - jv_total_amount)
            difference_order_value = abs(db_order_value - jv_order_value)
            difference_service_charges = abs(db_service_charges - jv_service_charges)
            difference_gst = abs(db_gst - jv_gst)

            status = "discrepancy"

            differences = []
            if difference_total_amount > tolerance:
                differences.append(f"DB Total: {db_total_amount}, JV Total: {jv_total_amount}, Difference in Total Amount: {difference_total_amount}.")
            if difference_order_value > tolerance:
                differences.append(f"DB Order Value: {db_order_value}, JV Order Value: {jv_order_value}, Difference in Order Value: {difference_order_value}.")
            if difference_service_charges > tolerance:
                differences.append(f"DB Service Charge: {db_service_charges}, JV Service Charges: {jv_service_charges}, Difference in Service Charge: {difference_service_charges}.")
            if difference_gst > tolerance:
                differences.append(f"DB GST: {db_gst}, JV GST: {jv_gst}, Difference in GST Value: {difference_gst}.")

            if len(differences) == 0:
                status = "valid"
    
            return{
                "status": status,
                "remark": " ".join(differences)
            }
            
        elif file_type == "Modification JV":
            validation_query = """
            SELECT 
                SUM(amount) AS total_amount
            FROM
                "tabNPS Charge"
            WHERE 
                creation::DATE = %s
                AND status = 'captured'
            """

            query_result = frappe.db.sql(validation_query, (doc.from_date,), as_dict=True)

            if not query_result:
                return{
                    "status": "error",
                    "remark": "No matching data found in NPS Charge table for this data"
                }
            
            db_total_amount = float(query_result[0].get("total_amount", 0) or 0)
            jv_total_amount = float(doc.total_amount or 0)

            difference_total_amount = abs(db_total_amount - jv_total_amount)

            if difference_total_amount <= tolerance:
                return{
                    "status": "valid",
                    "remark": "No discrepancies found."
                }
            else:
                return{
                    "status": "discrepancy",
                    "remark": f"DB Total Amount:{db_total_amount}, JV Total Amount:{jv_total_amount}, Difference:{difference_total_amount}"
                }            

        elif file_type == "Comparison":
            return _fetch_payment_difference(doc)
            
        raise Exception("Invalid file type.")
    
    except Exception as e:
        frappe.log_error(f"Validation error: {str(e)}", "NPS Validation Error")
        return {
            "status": "error",
            "difference": f"Validation error: {str(e)}",
            "remark": f"Error: {str(e)}",
            "record_count": 0
        }
    
def _fetch_payment_difference(doc):
    filepath = "{}{}".format(frappe.get_site_path(), doc.uploaded_file)

    expected_columns = {
        "entity_id", "type", "debit", "credit", "amount", "currency", 
        "fee", "tax", "on_hold", "settled", "created_at", "settled_at", 
        "settlement_id", "description", "notes", "payment_id", "arn", 
        "settlement_utr", "order_id", "order_receipt", "method", 
        "upi_flow", "card_network", "card_issuer", "card_type", 
        "dispute_id", "additional_utr"
    }

    with open(os.path.abspath(filepath)) as csvfile:
        reader = csv.reader(csvfile)
        actual_columns = next(reader) #first row -> headers
        actual_columns = {col.strip().lower() for col in actual_columns}
        #print(actual_columns)

    if actual_columns != expected_columns:
        return{
            "status": "error",
            "remark": "Invalid file for comparison.",
            "record_count": 0
        }
    
    cmd = "cp {} /tmp/transact.csv".format(os.path.abspath(filepath))
    os.system(cmd)

    file_doc = frappe.get_doc("File", {"file_url": doc.uploaded_file})
    is_agent = 'agent' in file_doc.file_name.lower()

    remarks = "No discrepancies found."
    total_discrepancies = 0

    create_temp_table_query = """
        CREATE TEMP TABLE "tmp_transaction"(
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
            created_at text, 
            settled_at text, 
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
        );
    """

    load_data_query = """
       COPY tmp_transaction FROM '/tmp/transact.csv' DELIMITER ',' CSV HEADER;
    """

    missing_agent_contribution_query = """
        SELECT t.order_id
        FROM tmp_transaction t
        LEFT JOIN tabnps_agent_contribution c
            ON t.entity_id = (c.payment_aggregator_meta->>'reference_id')
        WHERE t.entity_id LIKE 'pay_%'
            AND t.order_id IS NOT NULL
            AND t.order_receipt IS NOT NULL
            AND (c.payment_aggregator_meta->>'reference_id') IS NULL;
    """

    missing_contribution_query = """
        SELECT t.order_id 
        FROM tmp_transaction t
        LEFT JOIN tabnps_contribution c
            ON t.order_id = c.order_id
        WHERE t.order_id IS NOT NULL
            AND t.settled_at IS NOT NULL
            AND c.order_id IS NULL;
    """

    frappe.db.sql(create_temp_table_query)
    frappe.db.sql(load_data_query)
    orders = frappe.db.sql(missing_agent_contribution_query if is_agent else missing_contribution_query, as_dict=True)

    if len(orders):
        total_discrepancies = len(orders)
        order_ids = [row['order_id'] for row in orders]
        
        remarks = "Discrepancies found for {count} records. Missing Order IDs: {order_ids}".format(
            count=len(orders),
            order_ids=", ".join(order_ids)
        )

    return{
        "status": "success",
        "remark": "".join(remarks),
        "record_count": total_discrepancies
    }
