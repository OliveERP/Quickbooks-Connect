# Quickbooks-Connect

<h2>Setup Pre-Requisites</h2>

<b>1. Creation of two fields(Longitude, and Latitude) in Doctype(ADDRESS)</b>
       1. Go to "Address" and click on go to 'Customize Form' from Menu (three dots)
       
       2. Create two fields at convenient location:
       
          ![image](https://user-images.githubusercontent.com/120718232/213418877-0255bdca-2842-4ddd-896f-0dabcd53aea8.png)
       
       
       3. Click on update button and refresh the system with 'F5 key'
       
<b>2. Make sure field: Quickbooks ID, has "Allow on submit" checked in all of the forms</b>
       
       For instance, the 'Quickbooks ID' field will be present on the following forms:
       
       1. Item
       
       2. Customer
       
       3. Supplier
       
       4. Sales Invoice
       
       5. Purchase Invoice
       
       6. Address
       
       7. Journal Entry
       
       In each Customize form, click on edit of "Quickbooks ID" and check the "Allow on Submit" checkbox
       ![image](https://user-images.githubusercontent.com/120718232/213420340-8c5ac925-4a70-486b-b6d9-188d487ae906.png)
       
       Update the form and refresh the system to ensure changes.

<b>3. Creation of a Client Script for 'Quickbooks Migrator'</b>
      
      1. Add a Client Script
      
      2. Type 'QuickBooks Migrator' in Doctype
      
      3. Select 'Form' in Apply To
      
      4. Click the Enabled checkbox
      
      5. Copy and Paste the script 'QuickBooks Migrator' in it and Save
      
<b>4. Customer Group "Commercial" should exists in ERPNext</b>
       Go to "Customer Group Tree' and check if "Commercial" exists under "All Customer Group".
       ![image](https://user-images.githubusercontent.com/120718232/213416987-e712dab5-64e1-450b-a213-ad54cbf6956e.png)
       
       If not, then click on 'Add New' as shown above and create a new customer group "Commercial"

<b>5. Item Group "All Item Groups" should exists in ERPNext</b>
       Go to "Item Group Tree" and check if "All Item Groups" exists. If not, click on 'Add Child' and create a new item group "All Item Groups"
       
<b>6. Supplier Group "All Supplier Groups" should exists in ERPNext</b>
       Go to "Supplier Group Tree" and check if "All Supplier Groups" exists. If not, click on "Add Child" and create a new supplier group "All Supplier Groups"
       
<b>7. Territories "All Territories" should exists in ERPNext</b>
       Go to "Territories Tree" and check if "All Territories" exists. If not, click on "Add Child" and create a new territory "All Territories"
