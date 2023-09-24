import gspread
from oauth2client.service_account import ServiceAccountCredentials


def column_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


class GoogleSheets():

    def load_spreadsheet(self, sheet_id):
        self.sheet = self.sheets_connection.open_by_key(sheet_id)

    def load_worksheet(self, worksheet_name):
        self.worksheet = self.sheet.worksheet(worksheet_name)

    def load_worksheet_content(self, group_by=None):
        rows = self.sheet.get_all_values()
        keys = rows[0]

        rows_output = []

        # Delete header
        del rows[0]

        for row in rows:
            row_output = {}
            for col_index in range(len(row)):
                row_output[keys[col_index]] = row[col_index]
            rows_output.append(row_output)

        if group_by == None:
            return rows_output

        grouped_output = {}
        for row in rows_output:
            grouped_output[row[group_by]] = row

        return grouped_output

    def clear_contents(self):
        self.worksheet.clear()

    def clear_range(self, data_range):
        self.sheet.values_clear(data_range)

    def upload_dataframe(self, df):
        headers = list(df.columns)
        columns = len(df.columns)
        rows = len(df) + 2
        df_list = df.values.tolist()
        header_range = f"A1:{column_string(columns)}1"
        data_range = f"A2:{column_string(columns)}{str(rows)}"
        self.worksheet.batch_update([{
            'range': header_range,
            'values': [headers]
        },
            {
                'range': data_range,
                'values': df_list,
            }], value_input_option='USER_ENTERED')

    def upload_dataframe_split(self, df1, df2, header_range1, header_range2, datarange1, datarange2):
        headers1 = list(df1.columns)
        headers2 = list(df2.columns)
        df1_list = df1.values.tolist()
        df2_list = df2.values.tolist()
        print('To update first df ...')
        self.worksheet.batch_update([{
            'range': header_range1,
            'values': [headers1]
        },
            {
                'range': datarange1,
                'values': df1_list,
            }], value_input_option='USER_ENTERED')

        print('To update second df ...')

        self.worksheet.batch_update([{
            'range': header_range2,
            'values': [headers2]
        },
            {
                'range': datarange2,
                'values': df2_list,
            }], value_input_option='USER_ENTERED')

    def update_cell(self, cell_ref, value):
        self.worksheet.update(cell_ref, value)

    def __init__(self):
        google_service_account_path = 'config/gsheets-google-service-account.json'
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(google_service_account_path, scopes)
        self.sheets_connection = gspread.authorize(self.credentials)
