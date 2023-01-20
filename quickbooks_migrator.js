frappe.ui.form.on('QuickBooks Migrator', {
	refresh(frm) {
		if (frm.doc.access_token) {
			if (frm.doc.company) {
				frm.add_custom_button(__("Fetch Data"), function () {
					frm.trigger("fetch_data")
					frm.set_value("last_fetched", frappe.datetime.now_datetime())
					frm.save()
				});
				frm.add_custom_button(__("Sync Data"), function () {
					frm.call({
						"method":"qb_post",
						doc:frm.doc,
						callback: function(r) {
							console.log(r.message);
							frm.set_value("status", "Complete");
							frm.set_value("last_synced", frappe.datetime.now_datetime())
							frm.save();
						}
					});
				});
			}
		}
	}
});
