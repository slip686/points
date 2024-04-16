from itertools import islice
from datetime import datetime

from openpyxl import load_workbook
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs

from settings import get_settings


def parse_file(filename: str = None) -> list:
    if filename:
        book = load_workbook(filename, read_only=True)
        work_sheet = book[book.sheetnames[0]]

        for row in islice(work_sheet.rows, 1, None):
            row_data = []
            cell = None
            try:
                for cell in row:
                    cell = cell
                    value = str(cell.value).replace(',', '.')
                    if cell.column == 5:
                        datetime.fromisoformat(value)
                        row_data.append(value)
                    else:
                        float(value)
                        row_data.append(value)
            except ValueError:
                print(f"Invalid value in cell {cell.coordinate}")
                break
            yield row_data


def upload_data(filename: str = None):
    with psycopg2.connect(dsn=get_settings().db.dsn) as conn:
        with conn.cursor() as cur:

            sql_points_expr = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(sql.Identifier('track_point_model'),
            sql.SQL(', ').join(map(sql.Identifier, ['id', 'point', 'speed', 'gps_time', 'vehicle_id'])),
            sql.SQL(', ').join(map(sql.Literal, [AsIs(f"({n[0]}, 'POINT({n[2]} {n[1]})', {n[3]}, '{n[4]}', {n[5]})") for n in parse_file(filename)])))

            sql_vehicles_expr = sql.SQL("INSERT INTO {} ({}) VALUES {} ON CONFLICT DO NOTHING").format(
                            sql.Identifier('vehicle_model'),
                                sql.Identifier('vehicle_id'),
                                sql.SQL(', ').join(map(sql.Literal, [AsIs(f'({n[5]})') for n in parse_file(filename)])))
            try:
                cur.execute(sql_vehicles_expr)
                cur.execute(sql_points_expr)
                conn.commit()
            except psycopg2.Error as e:
                print(e)
                conn.rollback()


if __name__ == '__main__':
    upload_data("/Users/slip686/Downloads/2_5420464171701519891.xlsx")
