frappe.query_reports["join_total2"]{
	filters:[
		{
			fieldname: "from",
			label: "From",
			fieldtype: "Date",
			default: frappe.datetime.add_days(frappe.datetime.get_today()),
			reqd:1
		},
		{
			fieldname: "to",
			label: "To",
			fieltype: "Date",
			default: frappe.datetime.get_today(),
			reqd: 1
		}
	]
};
