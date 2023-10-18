import csv
import sqlite3


def insert_0():
    con = sqlite3.connect("shipment_database.db")
    cur = con.cursor()
    with open("data/shipping_data_0.csv") as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            (origin, dest, product, on_time, quantity, driver_id) = row
            cur.execute("INSERT OR IGNORE INTO product (name) "
                        f"VALUES ('{product}')")
            res = cur.execute(f"SELECT * FROM product WHERE name = '{product}'")
            product_id = res.fetchall()[0][0]
            cur.execute("INSERT OR IGNORE INTO shipment (product_id, quantity, origin, destination) "
                        f"VALUES ({product_id}, {quantity}, '{origin}', '{dest}')")
    con.commit()


def insert_1_and_2():
    con = sqlite3.connect("shipment_database.db")
    cur = con.cursor()
    with open("data/shipping_data_1.csv") as csvfile1, open("data/shipping_data_2.csv") as csvfile2:
        next(csvfile1)
        next(csvfile2)
        ship_prod = {}
        product_quant = {}
        reader1 = csv.reader(csvfile1)
        for row in reader1:
            (shipment_id, product, _) = row
            if shipment_id in ship_prod:
                ship_prod[shipment_id].add(product)
            else:
                ship_prod[shipment_id] = set()
                ship_prod[shipment_id].add(product)

            if (shipment_id, product) in product_quant:
                product_quant[(shipment_id, product)] = 1 + product_quant[(shipment_id, product)]
            else:
                product_quant[(shipment_id, product)] = 1

        reader2 = csv.reader(csvfile2)
        for row in reader2:
            (shipment_id, origin, dest, _) = row
            for product in ship_prod[shipment_id]:
                cur.execute("INSERT OR IGNORE INTO product (name) "
                            f"VALUES ('{product}')")
                res = cur.execute(f"SELECT * FROM product WHERE name = '{product}'")
                product_id = res.fetchall()[0][0]
                cur.execute("INSERT OR IGNORE INTO shipment (product_id, quantity, origin, destination) "
                            f"VALUES ({product_id}, {product_quant[(shipment_id, product)]}, '{origin}', '{dest}')")
    con.commit()


if __name__ == "__main__":
    insert_0()
    insert_1_and_2()
