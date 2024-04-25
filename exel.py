import csv
import time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import creds_info, spreadsheet_id, sheet_id
from postgres import check_purchase_length
from selen import sel


def google_sheets_data(creds_info, spreadsheet_id, sheet_name):
    data = []
    creds = Credentials.from_authorized_user_info(creds_info)
    service = build("sheets", "v4", credentials=creds)

    range_name = f"{sheet_name}!A2:H"  # Диапазон, в котором нужно проверить столбец H

    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            print("Таблица пуста.")
            return
        else:
            for i, row in enumerate(values, start=2):
                if len(row) <= 7:
                    data.append((i, row))
        return data
    except HttpError as e:
        print(f"Произошла ошибка: {e}")

def update_status_in_google_sheet(row_number, status):
    creds = Credentials.from_authorized_user_info(creds_info)
    service = build("sheets", "v4", credentials=creds)

    row_range = f"{sheet_id}!H{row_number}"

    value_range_body = {
        "values": [[status]]
    }

    try:
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=row_range,
            valueInputOption="RAW",
            body=value_range_body
        )
        response = request.execute()
        print("Статус обновлен в Google таблице.")
    except HttpError as e:
        print(f"Произошла ошибка при обновлении статуса: {e}")


def main():
    data = google_sheets_data(creds_info, spreadsheet_id, sheet_id)
    print(data)

    for row_number, item in data:
        email, rate_plan, class_year, lesson_level, subject, teacher_name, month = item
        first_name, last_name = teacher_name.split(' ', 1)
        result = check_purchase_length(subject, first_name, last_name, class_year, rate_plan, lesson_level, month)
        print(email,result)
        if result is not None and email is not None and result is not None:
            try:
                with open('add_products.csv', 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=';')
                    writer.writerow(["email", "product"])
                    writer.writerow([email, result])
                time.sleep(10)
                sel()
            except:
                update_status_in_google_sheet(row_number, "Ошибка")

                continue


            update_status_in_google_sheet(row_number, "Открыт")

        else:
            update_status_in_google_sheet(row_number, "Ошибка")

main()
