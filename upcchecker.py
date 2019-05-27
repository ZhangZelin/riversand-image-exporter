import glob
import os
import pyodbc
from azure.storage.blob import BlockBlobService


def change_char(s, r):
    return s[:-1]+r


def process_filename(e_dict, u_dict, filename):
    file_upc, file_ext = os.path.splitext(os.path.basename(filename))
    e_dict[file_upc] = file_ext
    return process_underscore(u_dict, os.path.splitext(os.path.basename(filename))[0])


def process_underscore(u_dict, filename):
    if '_' in filename:
        file_upc, underscore_num = filename.split('_')
        if file_upc not in u_dict:
            u_dict[file_upc] = [underscore_num]
        else:
            u_dict[file_upc] = u_dict[file_upc]+[underscore_num]
        return file_upc
    else:
        return filename


extension_dict = {}
underscore_dict = {}
upc = [process_filename(extension_dict, underscore_dict, x) for x in glob.glob("images/*")]
upc_string = '('

for file in upc:
    upc_string += (file + ',')

upc_string = change_char(upc_string, ')')
vendor_blacklist = ['453']
blacklist_string = '('

for blacklisted in vendor_blacklist:
    blacklist_string += (blacklisted + ',')
blacklist_string = change_char(blacklist_string, ')')

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=KL-ARAMANATHAN2\\SQLEXPRESS;'
                      'Database=nissDBClone;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
query_string = 'SELECT [nissDBClone].[dbo].[waveUPCs].[UPC],[Vendor_id] FROM [nissDBClone].[dbo].[waveUPCs] INNER JOIN [nissDBClone].[dbo].[Article] ON [nissDBClone].[dbo].[waveUPCs].UPC = [nissDBClone].[dbo].[Article].[UPC] WHERE [nissDBClone].[dbo].[waveUPCs].[UPC] IN ' + upc_string + ' AND [nissDBClone].[dbo].[Article].[Vendor_id] NOT IN ' + blacklist_string
cursor.execute(query_string)

block_blob_service = BlockBlobService(account_name='nissexportabyclq6lrbxyi', account_key='UyMf2eL+4D1/RBHLUSOZPOGQPLv4qjMNquxWXSucnmzJ8B0+DUtku2K9Xzby8SObyj/DgnZ2hh0k2wLEOkdjBg==')


def upload_row(name):
    local_path = os.path.abspath(os.path.curdir) + '\\images'
    local_file_name = name + extension_dict[name]
    full_path_to_file = os.path.join(local_path, local_file_name)
    block_blob_service.create_blob_from_path('images', local_file_name, full_path_to_file)
    print("Uploaded:" + name + extension_dict[name])


for item in cursor:
    if item[0] in underscore_dict:
        upload_row(item[0])
        for var in underscore_dict[item[0]]:
            upload_row(item[0] + '_' + var)
    else:
        upload_row(item[0])