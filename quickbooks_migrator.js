frappe.ui.form.on('QuickBooks Migrator', {
	refresh(frm) {
		if (frm.doc.access_token) {
			if (frm.doc.company) {
				frm.add_custom_button(__("Sync Data"), function () {
					frm.call({
						"method":"qb_post",
						doc:frm.doc,
						callback: function(r) {
							console.log(r.message);
							frm.set_value("status", "Complete");
							frm.save();
						}
					});
				});
			}
		}
	}
});
