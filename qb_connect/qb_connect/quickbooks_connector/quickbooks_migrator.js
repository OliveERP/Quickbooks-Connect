frappe.ui.form.on('QuickBooks Migrator', {
	refresh: function(frm) {
		frm.trigger("set_indicator")
		if (!frm.doc.access_token) {
			if (frm.doc.authorization_url) {
				frm.add_custom_button(__("Connect to Quickbooks"), function () {
					frm.trigger("connect")
				});
			}
		}
		if (frm.doc.access_token) {
			// If we have access_token that means we also have refresh_token we don't need user intervention anymore
			// All we need now is a Company from erpnext
			frm.remove_custom_button(__("Connect to Quickbooks"))
			frm.toggle_display("company_settings", 1)
			frm.set_df_property("company", "reqd", 1)
			if (frm.doc.company) {
					frm.add_custom_button(__("Fetch Data"), function () {
					frm.trigger("fetch_data")
				});
				frm.add_custom_button(__("Sync Data"), function () {
					frm.call({
						"method":"qb_post",
						doc:frm.doc,
						callback: function(r) {
							console.log(r.message)
							frm.set_value("status", "Complete")
							frm.save()
						}
					})
				});
			}
		}
	}
});