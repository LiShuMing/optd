#!/usr/bin/env python3
"""Generate an explicit non-dsdgen input witness for one unchanged TPC-DS query.

Only /tmp output is allowed. The original fixed manifest, all 99 SQL identities,
and typed schema are checked before writing. No SQL or verifier is rewritten.
"""

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re


FIXED_MANIFEST_SHA256 = "f175d057e27e3575dcff1b74ae6f246a0b7e6610fd2ce1be93aa043d59a27935"
NULL = "\\N"
VERSION = 1


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def calendar(key, text, **extra):
    value = date.fromisoformat(text)
    return {"d_date_sk": key, "d_date_id": text, "d_date": text,
            "d_year": value.year, "d_moy": value.month, "d_dom": value.day,
            "d_qoy": (value.month - 1) // 3 + 1,
            "d_month_seq": (value.year - 1900) * 12 + value.month - 1, **extra}


def customer(key=1, **extra):
    return {"c_customer_sk": key, "c_customer_id": f"CUSTOMER{key}",
            "c_first_name": "First", "c_last_name": f"Last{key}",
            "c_current_addr_sk": 1, "c_preferred_cust_flag": "Y", **extra}


def item(key=1, **extra):
    return {"i_item_sk": key, "i_item_id": f"ITEM{key}", "i_item_desc": f"Item {key}",
            "i_product_name": f"Product {key}", "i_current_price": "75.00",
            "i_brand_id": key, "i_brand": f"Brand {key}", "i_manufact": "Witness Manufacturer", **extra}


def sale(channel, key=1, amount="100.00", **extra):
    customer_column = "ss_customer_sk" if channel == "ss" else f"{channel}_bill_customer_sk"
    return {f"{channel}_sold_date_sk": key, f"{channel}_item_sk": 1,
            customer_column: 1, f"{channel}_quantity": 1,
            f"{channel}_ext_list_price": amount, f"{channel}_ext_sales_price": amount,
            f"{channel}_ext_wholesale_cost": "0.00", f"{channel}_ext_discount_amt": "0.00",
            **extra}


def recipe(query_id):
    if query_id == 1:
        return {"date_dim": [calendar(1, "2000-01-15")],
                "store": [{"s_store_sk": 1, "s_state": "TN"}], "customer": [customer(1), customer(2)],
                "store_returns": [{"sr_customer_sk": key, "sr_store_sk": 1, "sr_returned_date_sk": 1,
                    "sr_return_amt": amount} for key, amount in [(1, "100.00"), (2, "0.00")]]}, "TN customer return100 exceeds1.2 times same-store mean50"
    if query_id == 2:
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        return {"date_dim": [calendar(key + 1, f"{year}-01-{first_day + day:02}", d_week_seq=week,
                    d_day_name=name) for year, week, offset, first_day in [(2001, 1, 0, 7), (2002, 54, 7, 13)]
                    for day, name in enumerate(days) for key in [offset + day]],
                "web_sales": [sale("ws", key, amount="100.00") for key in range(1, 15)],
                "catalog_sales": [sale("cs", key, amount="100.00") for key in range(1, 15)]}, "All seven weekday revenues in original2001/2002 week-pair separated by53"
    if query_id == 5:
        return {"date_dim": [calendar(1, "2000-09-01")],
                "store": [{"s_store_sk": 1, "s_store_id": "STORE1"}],
                "catalog_page": [{"cp_catalog_page_sk": 1, "cp_catalog_page_id": "PAGE1"}],
                "web_site": [{"web_site_sk": 1, "web_site_id": "SITE1"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_net_profit="10.00")],
                "store_returns": [{"sr_store_sk": 1, "sr_returned_date_sk": 1, "sr_return_amt": "5.00", "sr_net_loss": "1.00"}],
                "catalog_sales": [sale("cs", cs_catalog_page_sk=1, cs_net_profit="10.00")],
                "catalog_returns": [{"cr_catalog_page_sk": 1, "cr_returned_date_sk": 1, "cr_return_amount": "5.00", "cr_net_loss": "1.00"}],
                "web_sales": [sale("ws", ws_web_site_sk=1, ws_order_number=1, ws_net_profit="10.00")],
                "web_returns": [{"wr_item_sk": 1, "wr_order_number": 1, "wr_returned_date_sk": 1,
                    "wr_return_amt": "5.00", "wr_net_loss": "1.00"}]}, "Original Aug23-Sep6 sales/returns in all three channels and rollup"
    if query_id == 6:
        return {"date_dim": [calendar(1, "2001-01-15")], "customer": [customer()],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "TN"}],
                "item": [item(key, i_category="Books", i_current_price=price) for key, price in [(1, "100.00"), (2, "1.00")]],
                "store_sales": [sale("ss") for _ in range(10)]}, "Original count>=10 and high-price>1.2 times category mean"
    if query_id == 7:
        return {"item": [item()], "date_dim": [calendar(1, "2000-01-15")],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "M", "cd_marital_status": "S", "cd_education_status": "College"}],
                "promotion": [{"p_promo_sk": 1, "p_channel_email": "N"}],
                "store_sales": [sale("ss", ss_cdemo_sk=1, ss_promo_sk=1, ss_list_price="75.00", ss_coupon_amt="0.00", ss_sales_price="50.00")]}, "Original male/single/college promotion filter and nonNULL averages"
    if query_id == 9:
        return {"reason": [{"r_reason_sk": 1}],
                "store_sales": [sale("ss", ss_quantity=quantity, ss_net_paid="100.00") for quantity in [1, 21, 41, 61, 81]]}, "Five original quantity buckets use unchanged threshold ELSE branches"
    if query_id in (12, 20):
        channel, table = ("ws", "web_sales") if query_id == 12 else ("cs", "catalog_sales")
        return {"item": [item(key, i_category="Books", i_class="Witness Class") for key in [1, 2]],
                "date_dim": [calendar(1, "1999-03-01")],
                table: [sale(channel, amount=amount, **{f"{channel}_item_sk": key})
                    for key, amount in [(1, "10.00"), (2, "30.00")]]}, "Original date/category and same-class revenue shares25/75"
    if query_id == 14:
        tables = {"item": [item(i_class_id=1, i_category_id=1)], "date_dim": [calendar(1, "2001-11-15")]}
        for channel, name in [("ss", "store_sales"), ("cs", "catalog_sales"), ("ws", "web_sales")]:
            tables[name] = [sale(channel, **{f"{channel}_list_price": "100.00"}) for _ in range(2)]
        return tables, "All three original intersected product dimensions and channel sum200 exceeds average100"
    if query_id == 15:
        return {"date_dim": [calendar(1, "2001-04-15")], "customer": [customer()],
                "customer_address": [{"ca_address_sk": 1, "ca_zip": "85669", "ca_state": "CA"}],
                "catalog_sales": [sale("cs", cs_sales_price="100.00")]}, "Original ZIP/state and2001Q2 catalog sum"
    if query_id == 17:
        return {"item": [item()], "store": [{"s_store_sk": 1, "s_state": "TN"}],
                "date_dim": [calendar(1, "2001-01-15", d_quarter_name="2001Q1")],
                "store_sales": [sale("ss", ss_store_sk=1, ss_ticket_number=1)],
                "store_returns": [{"sr_item_sk": 1, "sr_customer_sk": 1, "sr_ticket_number": 1, "sr_returned_date_sk": 1, "sr_return_quantity": 1}],
                "catalog_sales": [sale("cs")]}, "Original triple-channel customer/item/ticket join and quarter with count/average and sampleNULL"
    if query_id == 18:
        return {"date_dim": [calendar(1, "1998-01-15")], "item": [item()],
                "customer": [customer(c_birth_month=1, c_birth_year=1980, c_current_cdemo_sk=1)],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "F", "cd_education_status": "Unknown", "cd_dep_count": 2}],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "MS", "ca_country": "United States", "ca_county": "Witness"}],
                "catalog_sales": [sale("cs", cs_bill_cdemo_sk=1, cs_list_price="75.00", cs_coupon_amt="0.00", cs_sales_price="50.00", cs_net_profit="10.00")]}, "Original female/Unknown/birth-month/state filters and decimal averages through rollup"
    if query_id == 21:
        return {"item": [item(i_current_price="1.25")], "warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness"}],
                "date_dim": [calendar(1, "2000-03-01"), calendar(2, "2000-03-15")],
                "inventory": [{"inv_item_sk": 1, "inv_warehouse_sk": 1, "inv_date_sk": key, "inv_quantity_on_hand": 100} for key in [1, 2]]}, "Original price/date windows and before/after ratio1"
    if query_id == 22:
        return {"item": [item(i_class="Witness Class", i_category="Books")], "date_dim": [calendar(1, "2000-01-15")],
                "inventory": [{"inv_item_sk": 1, "inv_date_sk": 1, "inv_quantity_on_hand": 100}]}, "Original month1200 and positive inventory average through all rollup levels"
    if query_id == 26:
        return {"item": [item()], "date_dim": [calendar(1, "2000-01-15")],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "M", "cd_marital_status": "S", "cd_education_status": "College"}],
                "promotion": [{"p_promo_sk": 1, "p_channel_email": "N"}],
                "catalog_sales": [sale("cs", cs_bill_cdemo_sk=1, cs_promo_sk=1, cs_list_price="75.00", cs_coupon_amt="0.00", cs_sales_price="50.00")]}, "Original catalog male/single/college promotion filter and nonNULL averages"
    if query_id == 27:
        return {"item": [item()], "date_dim": [calendar(1, "2002-01-15")], "store": [{"s_store_sk": 1, "s_state": "TN"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "M", "cd_marital_status": "S", "cd_education_status": "College"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_cdemo_sk=1, ss_list_price="75.00", ss_coupon_amt="0.00", ss_sales_price="50.00")]}, "Original2002 TN demographics and explicit item/state/total union averages"
    if query_id == 28:
        return {"store_sales": [sale("ss", ss_quantity=quantity, ss_list_price=price)
                    for quantity, price in [(1, "8.00"), (6, "90.00"), (11, "142.00"), (16, "135.00"), (21, "122.00"), (26, "154.00")]]}, "All six original quantity/listprice branches with exact AVG/COUNT/DISTINCT"
    if query_id == 29:
        return {"item": [item()], "store": [{"s_store_sk": 1, "s_store_id": "STORE1", "s_store_name": "Witness"}],
                "date_dim": [calendar(1, "1999-09-15")], "store_sales": [sale("ss", ss_store_sk=1, ss_ticket_number=1)],
                "store_returns": [{"sr_item_sk": 1, "sr_customer_sk": 1, "sr_ticket_number": 1, "sr_returned_date_sk": 1, "sr_return_quantity": 1}],
                "catalog_sales": [sale("cs")]}, "Original September1999 sale/return/catalog customer-item-ticket join"
    if query_id == 33:
        return {"item": [item(i_category="Electronics", i_manufact_id=1)], "date_dim": [calendar(1, "1998-05-15")],
                "customer_address": [{"ca_address_sk": 1, "ca_gmt_offset": "-5.00"}],
                "store_sales": [sale("ss", ss_addr_sk=1)], "catalog_sales": [sale("cs", cs_bill_addr_sk=1)],
                "web_sales": [sale("ws", ws_bill_addr_sk=1)]}, "Original Electronics manufacturer subquery, May1998 and GMT-5 across three channels"
    if query_id == 34:
        return {"customer": [customer(c_salutation="Mr.")], "date_dim": [calendar(1, "1999-01-01")],
                "store": [{"s_store_sk": 1, "s_county": "Williamson County"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_buy_potential": "Unknown", "hd_vehicle_count": 1, "hd_dep_count": 2}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_hdemo_sk=1, ss_ticket_number=1) for _ in range(15)]}, "Original ticketcount15 in15..20 with dependent/vehicle ratio2 and Williamson County"
    if query_id == 35:
        return {"customer": [customer(c_current_cdemo_sk=1)], "date_dim": [calendar(1, "2002-01-15")],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "TN"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "F", "cd_marital_status": "S", "cd_dep_count": 1, "cd_dep_employed_count": 0, "cd_dep_college_count": 0}],
                "store_sales": [sale("ss")], "web_sales": [sale("ws")]}, "Original2002 first-three-quarter store and web EXISTS with demographics aggregates"
    if query_id == 36:
        return {"item": [item(i_category="Books", i_class="Witness Class")], "date_dim": [calendar(1, "2001-01-15")],
                "store": [{"s_store_sk": 1, "s_state": "TN"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_net_profit="10.00")]}, "Original2001 TN positive gross margin with explicit UNION hierarchy and rank"
    if query_id == 40:
        return {"item": [item(i_current_price="1.25")], "warehouse": [{"w_warehouse_sk": 1, "w_state": "TN"}],
                "date_dim": [calendar(1, "2000-03-01"), calendar(2, "2000-03-15")],
                "catalog_sales": [sale("cs", key, cs_warehouse_sk=1, cs_order_number=key, cs_sales_price="100.00") for key in [1, 2]],
                "catalog_returns": [{"cr_item_sk": 1, "cr_order_number": 1, "cr_refunded_cash": "10.00"}]}, "Original before/after date and price filters with returned and unreturned left-join sales"
    if query_id == 42:
        return {"item": [item(i_manager_id=1, i_category_id=1, i_category="Books")],
                "date_dim": [calendar(1, "2000-11-15")], "store_sales": [sale("ss")]}, "Original manager1 November2000 category revenue"
    if query_id == 43:
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        return {"store": [{"s_store_sk": 1, "s_store_name": "Witness", "s_store_id": "STORE1", "s_gmt_offset": "-5.00"}],
                "date_dim": [calendar(key+1, f"2000-01-{key+2:02}", d_day_name=name) for key, name in enumerate(days)],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price="100.00") for key in range(1, 8)]}, "Original2000 GMT-5 store and seven nonNULL weekday conditional sums"
    if query_id == 45:
        return {"customer": [customer()], "item": [item()], "date_dim": [calendar(1, "2001-04-15")],
                "customer_address": [{"ca_address_sk": 1, "ca_zip": "85669", "ca_city": "Witness"}],
                "web_sales": [sale("ws", ws_sales_price="100.00")]}, "Original ZIP and2001Q2 web revenue grouped by city"
    if query_id == 46:
        return {"date_dim": [calendar(1, "1999-01-03", d_dow=0)], "store": [{"s_store_sk": 1, "s_city": "Fairview"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 4}], "customer": [customer(c_current_addr_sk=2)],
                "customer_address": [{"ca_address_sk": 1, "ca_city": "Bought"}, {"ca_address_sk": 2, "ca_city": "Current"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_hdemo_sk=1, ss_addr_sk=1, ss_ticket_number=1, ss_coupon_amt="1.00", ss_net_profit="10.00")]}, "Original weekend/Fairview/dependents filters and changed current city"
    if query_id == 47:
        return {"item": [item(i_category="Books")], "store": [{"s_store_sk": 1, "s_store_name": "Witness", "s_company_name": "Witness Company"}],
                "date_dim": [calendar(key, day) for key, day in [(1, "1998-12-15"), (2, "1999-01-15"), (3, "1999-02-15")]],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price=amount) for key, amount in [(1, "100.00"), (2, "100.00"), (3, "300.00")]]}, "Original rank-adjacent months and January1999 deviation100 versus mean200"
    if query_id == 48:
        return {"date_dim": [calendar(1, "2000-01-15")], "store": [{"s_store_sk": 1}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "M", "cd_education_status": "4 yr Degree"}],
                "customer_address": [{"ca_address_sk": 1, "ca_country": "United States", "ca_state": "TX"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_cdemo_sk=1, ss_addr_sk=1, ss_sales_price="125.00", ss_net_profit="100.00")]}, "Original marital/education/price and US/state/profit conjunction"
    if query_id == 61:
        return {"item": [item(i_category="Jewelry")], "date_dim": [calendar(1, "1998-11-15")], "customer": [customer()],
                "customer_address": [{"ca_address_sk": 1, "ca_gmt_offset": "-5.00"}],
                "store": [{"s_store_sk": 1, "s_gmt_offset": "-5.00"}],
                "promotion": [{"p_promo_sk": 1, "p_channel_dmail": "Y"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_promo_sk=1)]}, "Original Jewelry/date/GMT-5 direct-mail promotional revenue fraction100percent"
    if query_id == 62:
        return {"warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness Warehouse"}],
                "ship_mode": [{"sm_ship_mode_sk": 1, "sm_type": "Witness"}],
                "web_site": [{"web_site_sk": 1, "web_name": "Witness"}],
                "date_dim": [calendar(200, "2000-06-01")],
                "web_sales": [{"ws_ship_date_sk": 200, "ws_sold_date_sk": 200-gap,
                    "ws_warehouse_sk": 1, "ws_ship_mode_sk": 1, "ws_web_site_sk": 1} for gap in [10, 45, 75, 105, 130]]}, "Original shipment month window with five nonzero gap buckets"
    if query_id == 63:
        return {"item": [item(i_manager_id=1, i_category="Books", i_class="personal", i_brand="scholaramalgamalg #14")],
                "store": [{"s_store_sk": 1}], "date_dim": [calendar(1, "2000-01-15"), calendar(2, "2000-02-15")],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price=amount) for key, amount in [(1, "100.00"), (2, "300.00")]]}, "Original product/month filters; manager monthly sales100/300 deviate50percent from average200"
    if query_id == 64:
        return {"date_dim": [calendar(1, "1999-01-15"), calendar(2, "2000-01-15"), calendar(3, "1998-01-15")],
                "item": [item(i_color="purple", i_current_price="70.00")],
                "store": [{"s_store_sk": 1, "s_store_name": "Witness", "s_zip": "12345"}],
                "customer": [customer(c_current_cdemo_sk=2, c_current_hdemo_sk=1, c_first_sales_date_sk=3, c_first_shipto_date_sk=3)],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "M"}, {"cd_demo_sk": 2, "cd_marital_status": "S"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_income_band_sk": 1}], "income_band": [{"ib_income_band_sk": 1}],
                "customer_address": [{"ca_address_sk": 1, "ca_street_number": "1", "ca_street_name": "Main", "ca_city": "City", "ca_zip": "12345"}],
                "promotion": [{"p_promo_sk": 1}], "catalog_sales": [sale("cs", cs_order_number=1)],
                "catalog_returns": [{"cr_item_sk": 1, "cr_order_number": 1, "cr_refunded_cash": "1.00", "cr_reversed_charge": "1.00", "cr_store_credit": "1.00"}],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_ticket_number=key, ss_cdemo_sk=1, ss_hdemo_sk=1,
                    ss_addr_sk=1, ss_promo_sk=1, ss_wholesale_cost="10.00", ss_list_price="20.00", ss_coupon_amt="1.00") for key in [1, 2]],
                "store_returns": [{"sr_item_sk": 1, "sr_ticket_number": key} for key in [1, 2]]}, "Original cross-year store self-join, changed marital demographics and catalog margin100>2*3"
    if query_id == 65:
        return {"store": [{"s_store_sk": 1}], "item": [item(1), item(2)], "date_dim": [calendar(1, "1998-01-15")],
                "store_sales": [sale("ss", ss_store_sk=1, ss_item_sk=key, ss_sales_price=amount) for key, amount in [(1, "1.00"), (2, "100.00")]]}, "Original month1176 low product revenue1 below0.1 times mean50.5"
    if query_id == 66:
        return {"warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness", "w_city": "City", "w_county": "County", "w_state": "TN", "w_country": "US", "w_warehouse_sq_ft": 100}],
                "date_dim": [calendar(1, "2001-01-15")], "time_dim": [{"t_time_sk": 1, "t_time": 40000}],
                "ship_mode": [{"sm_ship_mode_sk": 1, "sm_carrier": "DHL"}],
                "web_sales": [sale("ws", ws_warehouse_sk=1, ws_sold_time_sk=1, ws_ship_mode_sk=1, ws_net_paid="100.00")],
                "catalog_sales": [sale("cs", cs_warehouse_sk=1, cs_sold_time_sk=1, cs_ship_mode_sk=1, cs_sales_price="200.00", cs_net_paid_inc_tax="200.00")]}, "Original DHL/time/year filters with nonzero channel monthly warehouse metrics"
    if query_id == 67:
        return {"item": [item(i_category="Books", i_class="Witness")], "store": [{"s_store_sk": 1, "s_store_id": "STORE1"}],
                "date_dim": [calendar(1, "2000-01-15")], "store_sales": [sale("ss", ss_store_sk=1, ss_sales_price="100.00")]}, "Original month window and ranked hierarchy rollup"
    if query_id == 68:
        return {"date_dim": [calendar(1, "1999-01-01")], "store": [{"s_store_sk": 1, "s_city": "Fairview"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 4}], "customer": [customer(c_current_addr_sk=2)],
                "customer_address": [{"ca_address_sk": 1, "ca_city": "Bought"}, {"ca_address_sk": 2, "ca_city": "Current"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_hdemo_sk=1, ss_addr_sk=1, ss_ticket_number=1, ss_ext_tax="1.00")]}, "Original Fairview/dependents filters and purchase city differs from current city"
    if query_id == 69:
        return {"customer": [customer(c_current_cdemo_sk=1)], "customer_address": [{"ca_address_sk": 1, "ca_state": "KY"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "F", "cd_marital_status": "S", "cd_education_status": "College", "cd_purchase_estimate": 100, "cd_credit_rating": "Good", "cd_dep_count": 1, "cd_dep_employed_count": 0, "cd_dep_college_count": 0}],
                "date_dim": [calendar(1, "2001-04-15")], "store_sales": [sale("ss")]}, "Original KY customer store EXISTS and both other-channel NOT EXISTS"
    if query_id == 70:
        return {"date_dim": [calendar(1, "2000-01-15")], "store": [{"s_store_sk": 1, "s_state": "GA", "s_county": "Witness"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_net_profit="100.00")]}, "Original month window; single-state rank1 and county/state rollup"
    if query_id == 71:
        return {"item": [item(i_manager_id=1)], "date_dim": [calendar(1, "1999-11-15")],
                "time_dim": [{"t_time_sk": 1, "t_hour": 8, "t_minute": 30, "t_meal_time": "breakfast"}],
                **{table: [sale(channel, amount=amount, **{f"{channel}_sold_time_sk": 1})]
                    for channel, table, amount in [("ss", "store_sales", "100.00"), ("cs", "catalog_sales", "200.00"), ("ws", "web_sales", "300.00")]}}, "Original manager/November/breakfast union of three channel revenues"
    if query_id == 72:
        return {"item": [item()], "warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "D"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_buy_potential": ">10000"}],
                "date_dim": [calendar(1, "1999-01-01", d_week_seq=1), calendar(2, "1999-01-08", d_week_seq=2)],
                "inventory": [{"inv_item_sk": 1, "inv_warehouse_sk": 1, "inv_date_sk": 1, "inv_quantity_on_hand": 1}],
                "promotion": [{"p_promo_sk": 1}], "catalog_sales": [sale("cs", cs_quantity=2, cs_ship_date_sk=2,
                    cs_bill_cdemo_sk=1, cs_bill_hdemo_sk=1, cs_order_number=1, cs_promo_sk=1)]}, "Original inventory shortage, demographic, week and shipping>5days conditions"
    if query_id == 73:
        return {"customer": [customer(c_salutation="Mr.")], "date_dim": [calendar(1, "1999-01-01")],
                "store": [{"s_store_sk": 1, "s_county": "Orange County"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_buy_potential": "Unknown", "hd_vehicle_count": 1, "hd_dep_count": 2}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_hdemo_sk=1, ss_ticket_number=1)]}, "Original county/household conditions and ticketcount1 in1..5"
    if query_id == 74:
        return {"customer": [customer()], "date_dim": [calendar(1, "2001-01-15"), calendar(2, "2002-01-15")],
                "store_sales": [sale("ss", key, ss_net_paid=amount) for key, amount in [(1, "100.00"), (2, "110.00")]],
                "web_sales": [sale("ws", key, ws_net_paid=amount) for key, amount in [(1, "100.00"), (2, "200.00")]]}, "Original2001/2002 webgrowth2 exceeds storegrowth1.1"
    if query_id == 75:
        return {"item": [item(i_category="Books", i_class_id=1, i_category_id=1, i_manufact_id=1)],
                "date_dim": [calendar(1, "2001-01-15"), calendar(2, "2002-01-15")],
                "store_sales": [sale("ss", key, amount=amount, ss_quantity=quantity, ss_ticket_number=key)
                    for key, amount, quantity in [(1, "100.00", 10), (2, "50.00", 5)]]}, "Original Books product/year join and current/prior quantityratio0.5<0.9"
    if query_id == 76:
        return {"item": [item(i_category="Books")], "date_dim": [calendar(1, "2000-01-15")],
                "store_sales": [sale("ss", ss_store_sk=None)], "catalog_sales": [sale("cs", cs_ship_addr_sk=None)],
                "web_sales": [sale("ws", ws_ship_customer_sk=None)]}, "Original three channel NULL-column branches with nonNULL product/date joins"
    if query_id == 77:
        return {"date_dim": [calendar(1, "2000-09-01")], "store": [{"s_store_sk": 1}], "web_page": [{"wp_web_page_sk": 1}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_net_profit="10.00")],
                "store_returns": [{"sr_store_sk": 1, "sr_returned_date_sk": 1, "sr_return_amt": "5.00", "sr_net_loss": "1.00"}],
                "catalog_sales": [sale("cs", cs_call_center_sk=1, cs_net_profit="10.00")],
                "catalog_returns": [{"cr_call_center_sk": 1, "cr_returned_date_sk": 1, "cr_return_amount": "5.00", "cr_net_loss": "1.00"}],
                "web_sales": [sale("ws", ws_web_page_sk=1, ws_net_profit="10.00")],
                "web_returns": [{"wr_web_page_sk": 1, "wr_returned_date_sk": 1, "wr_return_amt": "5.00", "wr_net_loss": "1.00"}]}, "Original September2000 three-channel sales/returns and rollup"
    if query_id == 78:
        return {"date_dim": [calendar(1, "2000-01-15")],
                **{table: [sale(channel, **{f"{channel}_quantity": qty, f"{channel}_wholesale_cost": "1.00",
                    f"{channel}_sales_price": "10.00", f"{channel}_{order}": 1})]
                    for channel, table, order, qty in [("ss", "store_sales", "ticket_number", 10), ("cs", "catalog_sales", "order_number", 3), ("ws", "web_sales", "order_number", 2)]}}, "Original unreturned customer/item/year storequantity10 versus web2+catalog3"
    if query_id == 79:
        return {"customer": [customer()], "date_dim": [calendar(1, "1999-01-04", d_dow=1)],
                "store": [{"s_store_sk": 1, "s_city": "Witness", "s_number_employees": 250}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 6}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_hdemo_sk=1, ss_ticket_number=1, ss_coupon_amt="2.00", ss_net_profit="10.00")]}, "Original Monday/dependents/employee filters with ticket coupon aggregation"
    if query_id == 80:
        return {"date_dim": [calendar(1, "2000-09-01")], "item": [item()], "promotion": [{"p_promo_sk": 1, "p_channel_tv": "N"}],
                "store": [{"s_store_sk": 1, "s_store_id": "STORE1"}], "catalog_page": [{"cp_catalog_page_sk": 1, "cp_catalog_page_id": "PAGE1"}],
                "web_site": [{"web_site_sk": 1, "web_site_id": "SITE1"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_promo_sk=1, ss_ticket_number=1, ss_net_profit="10.00")],
                "catalog_sales": [sale("cs", cs_catalog_page_sk=1, cs_promo_sk=1, cs_order_number=1, cs_net_profit="10.00")],
                "web_sales": [sale("ws", ws_web_site_sk=1, ws_promo_sk=1, ws_order_number=1, ws_net_profit="10.00")]}, "Original three-channel price/promotion/date filters and unreturned outer-join rollup"
    if query_id == 90:
        return {"web_page": [{"wp_web_page_sk": 1, "wp_char_count": 5100}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 6}],
                "time_dim": [{"t_time_sk": 1, "t_hour": 8}, {"t_time_sk": 2, "t_hour": 19}],
                "web_sales": [{"ws_sold_time_sk": key, "ws_ship_hdemo_sk": 1, "ws_web_page_sk": 1} for key in [1, 2]]}, "Positive AM and PM counts under original page/household filters"
    if query_id == 91:
        return {"date_dim": [calendar(1, "1998-11-15")],
                "call_center": [{"cc_call_center_sk": 1, "cc_call_center_id": "CENTER1", "cc_name": "Witness Center", "cc_manager": "Witness Manager"}],
                "customer_address": [{"ca_address_sk": 1, "ca_gmt_offset": "-7.00"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "M", "cd_education_status": "Unknown"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_buy_potential": "Unknown"}],
                "customer": [customer(c_current_cdemo_sk=1, c_current_hdemo_sk=1)],
                "catalog_returns": [{"cr_call_center_sk": 1, "cr_returned_date_sk": 1, "cr_returning_customer_sk": 1, "cr_net_loss": "10.00"}]}, "Original November1998 unknown demographics and GMT-7 return loss"
    if query_id == 92:
        return {"item": [item(i_manufact_id=350)], "date_dim": [calendar(1, "2000-02-15")],
                "web_sales": [sale("ws", ws_ext_discount_amt=amount) for amount in ["10.00", "100.00"]]}, "Manufacturer350 high discount100 exceeds1.3 times correlated mean55"
    if query_id == 93:
        return {"reason": [{"r_reason_sk": 1, "r_reason_desc": "reason 28"}],
                "store_sales": [{"ss_item_sk": 1, "ss_ticket_number": 1, "ss_customer_sk": 1, "ss_quantity": 10, "ss_sales_price": "5.00"}],
                "store_returns": [{"sr_item_sk": 1, "sr_ticket_number": 1, "sr_reason_sk": 1, "sr_return_quantity": 2}]}, "Original reason28 joined return yields actual sales(10-2)*5"
    if query_id in (94, 95):
        tables = {"date_dim": [calendar(1, "1999-03-01")],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "IL"}],
                "web_site": [{"web_site_sk": 1, "web_company_name": "pri"}],
                "web_sales": [sale("ws", ws_order_number=1, ws_warehouse_sk=key, ws_ship_date_sk=1,
                    ws_ship_addr_sk=1, ws_web_site_sk=1, ws_ext_ship_cost="5.00", ws_net_profit="10.00") for key in [1, 2]]}
        if query_id == 95:
            tables["web_returns"] = [{"wr_order_number": 1}]
        return tables, "Original IL/pri date window two-warehouse order; returned only for Q95"
    if query_id == 96:
        return {"store": [{"s_store_sk": 1, "s_store_name": "ese"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 7}],
                "time_dim": [{"t_time_sk": 1, "t_hour": 20, "t_minute": 45}],
                "store_sales": [{"ss_store_sk": 1, "ss_hdemo_sk": 1, "ss_sold_time_sk": 1}]}, "Original ese store dependent7 and20:45 count"
    if query_id == 97:
        return {"date_dim": [calendar(1, "2000-01-15")],
                "store_sales": [sale("ss", ss_customer_sk=key, ss_item_sk=key) for key in [1, 3]],
                "catalog_sales": [sale("cs", cs_bill_customer_sk=key, cs_item_sk=key) for key in [2, 3]]}, "Distinct store-only/catalog-only/common customer-item pairs"
    if query_id == 98:
        return {"item": [item(key, i_category="Books", i_class="Witness Class") for key in [1, 2]],
                "date_dim": [calendar(1, "1999-03-01")],
                "store_sales": [sale("ss", amount=amount, ss_item_sk=key) for key, amount in [(1, "10.00"), (2, "30.00")]]}, "Two original-category items in same class with revenue shares25/75"
    if query_id == 99:
        return {"warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness Warehouse"}],
                "ship_mode": [{"sm_ship_mode_sk": 1, "sm_type": "Witness Ship Mode"}],
                "call_center": [{"cc_call_center_sk": 1, "cc_name": "MiXeD Center"}],
                "date_dim": [calendar(200, "2000-06-01")],
                "catalog_sales": [{"cs_ship_date_sk": 200, "cs_sold_date_sk": 200 - gap,
                    "cs_warehouse_sk": 1, "cs_ship_mode_sk": 1, "cs_call_center_sk": 1}
                    for gap in [10, 45, 75, 105, 130]]}, "Original month range and all five shipment-gap buckets"
    if query_id == 81:
        return {"customer": [customer(1), customer(2)],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "GA"}],
                "date_dim": [calendar(1, "2000-02-15")],
                "catalog_returns": [{"cr_returning_customer_sk": key, "cr_returning_addr_sk": 1,
                    "cr_returned_date_sk": 1, "cr_return_amt_inc_tax": amount}
                    for key, amount in [(1, "9000.00"), (2, "1.00")]]}, "Original GA catalog returns9000 exceeds1.2 times mean4500.5"
    if query_id == 82:
        return {"item": [item(i_manufact_id=129)], "date_dim": [calendar(1, "2000-06-01")],
                "inventory": [{"inv_item_sk": 1, "inv_date_sk": 1, "inv_quantity_on_hand": 200}],
                "store_sales": [sale("ss")]}, "Original manufacturer129 price75 quantity200 June2000 inventory and store purchase"
    if query_id == 83:
        return {"item": [item()], "date_dim": [calendar(1, "2000-06-30", d_week_seq=26)],
                "store_returns": [{"sr_item_sk": 1, "sr_returned_date_sk": 1, "sr_return_quantity": 3}],
                "catalog_returns": [{"cr_item_sk": 1, "cr_returned_date_sk": 1, "cr_return_quantity": 6}],
                "web_returns": [{"wr_item_sk": 1, "wr_returned_date_sk": 1, "wr_return_quantity": 9}]}, "Original anchor week with same-item returns3/6/9 in three channels"
    if query_id == 84:
        return {"customer": [customer(c_current_cdemo_sk=1, c_current_hdemo_sk=1)],
                "customer_address": [{"ca_address_sk": 1, "ca_city": "Edgewood"}],
                "customer_demographics": [{"cd_demo_sk": 1}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_income_band_sk": 1}],
                "income_band": [{"ib_income_band_sk": 1, "ib_lower_bound": 40000, "ib_upper_bound": 80000}],
                "store_returns": [{"sr_cdemo_sk": 1}]}, "Edgewood customer matching original income interval and return demographics"
    if query_id == 85:
        return {"date_dim": [calendar(1, "2000-02-15")], "web_page": [{"wp_web_page_sk": 1}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "M", "cd_education_status": "Advanced Degree"}],
                "customer_address": [{"ca_address_sk": 1, "ca_country": "United States", "ca_state": "IN"}],
                "reason": [{"r_reason_sk": 1, "r_reason_desc": "Witness reason"}],
                "web_sales": [sale("ws", ws_web_page_sk=1, ws_order_number=1, ws_quantity=2,
                                   ws_sales_price="125.00", ws_net_profit="150.00")],
                "web_returns": [{"wr_item_sk": 1, "wr_order_number": 1, "wr_refunded_cdemo_sk": 1,
                    "wr_returning_cdemo_sk": 1, "wr_refunded_addr_sk": 1, "wr_reason_sk": 1,
                    "wr_refunded_cash": "10.00", "wr_fee": "1.00"}]}, "Matching web return with original demographic, price and IN profit filters"
    if query_id == 86:
        return {"item": [item(i_category="Books", i_class="Witness Class")],
                "date_dim": [calendar(1, "2000-01-15")],
                "web_sales": [sale("ws", ws_net_paid="100.00")]}, "Original month range with positive net paid for rollup/rank"
    if query_id == 87:
        return {"customer": [customer()], "date_dim": [calendar(1, "2000-01-15")],
                "store_sales": [sale("ss")]}, "Store-only customer/date survives both original EXCEPT relations"
    if query_id == 88:
        slots = [(8, 45), (9, 15), (9, 45), (10, 15), (10, 45), (11, 15), (11, 45), (12, 15)]
        return {"store": [{"s_store_sk": 1, "s_store_name": "ese"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 0, "hd_vehicle_count": 1}],
                "time_dim": [{"t_time_sk": key + 1, "t_hour": hour, "t_minute": minute} for key, (hour, minute) in enumerate(slots)],
                "store_sales": [{"ss_store_sk": 1, "ss_hdemo_sk": 1, "ss_sold_time_sk": key + 1} for key in range(len(slots))]}, "Original ese store and household conjunction populate every half-hour count"
    if query_id == 89:
        return {"item": [item(i_category="Books", i_class="computers")],
                "store": [{"s_store_sk": 1, "s_store_name": "Witness Store", "s_company_name": "Witness Company"}],
                "date_dim": [calendar(1, "1999-01-15"), calendar(2, "1999-02-15")],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price=amount)
                                for key, amount in [(1, "10.00"), (2, "100.00")]]}, "Original Books/computers1999 monthly sales10/100 deviate from mean55"
    if query_id == 49:
        return {"date_dim": [calendar(1, "2001-12-15")],
                "store_sales": [sale("ss", ss_ticket_number=1, ss_net_paid="20000.00", ss_net_profit="10.00")],
                "store_returns": [{"sr_item_sk": 1, "sr_ticket_number": 1, "sr_return_quantity": 1, "sr_return_amt": "15000.00"}],
                "catalog_sales": [sale("cs", cs_order_number=1, cs_net_paid="20000.00", cs_net_profit="10.00")],
                "catalog_returns": [{"cr_item_sk": 1, "cr_order_number": 1, "cr_return_quantity": 1, "cr_return_amount": "15000.00"}],
                "web_sales": [sale("ws", ws_order_number=1, ws_net_paid="20000.00", ws_net_profit="10.00")],
                "web_returns": [{"wr_item_sk": 1, "wr_order_number": 1, "wr_return_quantity": 1, "wr_return_amt": "15000.00"}]}, "Three-channel December2001 return15000 exceeds original10000 threshold; positive paid/quantity/profit"
    if query_id == 50:
        return {"store": [{"s_store_sk": 1, "s_store_name": "Witness Store"}],
                "date_dim": [calendar(1, "2001-07-15"), calendar(32, "2001-08-15")],
                "store_sales": [sale("ss", ss_store_sk=1, ss_ticket_number=1)],
                "store_returns": [{"sr_item_sk": 1, "sr_ticket_number": 1, "sr_customer_sk": 1, "sr_returned_date_sk": 32}]}, "Matching sale/return in August2001 with31-day key interval"
    if query_id == 51:
        return {"date_dim": [calendar(1, "2000-01-15")],
                "store_sales": [sale("ss", ss_sales_price="100.00")],
                "web_sales": [sale("ws", ws_sales_price="200.00")]}, "Same item/date web cumulative200 exceeds store100"
    if query_id in (52, 55):
        manager, year = (1, 2000) if query_id == 52 else (28, 1999)
        return {"item": [item(i_manager_id=manager)], "date_dim": [calendar(1, f"{year}-11-15")],
                "store_sales": [sale("ss")]}, "Original manager and November/year literal match"
    if query_id == 53:
        return {"item": [item(i_manufact_id=1, i_category="Books", i_class="personal", i_brand="scholaramalgamalg #14")],
                "store": [{"s_store_sk": 1}],
                "date_dim": [calendar(1, "2000-01-15"), calendar(2, "2000-04-15")],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price=amount)
                                for key, amount in [(1, "100.00"), (2, "300.00")]]}, "Original product attributes; quarterly100/300 differ from mean200 by50percent"
    if query_id == 54:
        return {"item": [item(i_category="Women", i_class="maternity")], "customer": [customer()],
                "customer_address": [{"ca_address_sk": 1, "ca_county": "Witness County", "ca_state": "TN"}],
                "store": [{"s_store_sk": 1, "s_county": "Witness County", "s_state": "TN"}],
                "date_dim": [calendar(1, "1998-12-15"), calendar(2, "1999-01-15")],
                "catalog_sales": [sale("cs")], "store_sales": [sale("ss", 2)]}, "December1998 maternity customer with following-quarter revenue in matching county/state"
    if query_id in (56, 60):
        product = item(i_color="slate") if query_id == 56 else item(i_category="Music")
        day = "2001-02-15" if query_id == 56 else "1998-09-15"
        return {"item": [product], "date_dim": [calendar(1, day)],
                "customer_address": [{"ca_address_sk": 1, "ca_gmt_offset": "-5.00"}],
                "store_sales": [sale("ss", ss_addr_sk=1)],
                "catalog_sales": [sale("cs", cs_bill_addr_sk=1)],
                "web_sales": [sale("ws", ws_bill_addr_sk=1)]}, "Three channels with original product/date and GMT-5 address filters"
    if query_id == 57:
        return {"item": [item(i_category="Books")],
                "call_center": [{"cc_call_center_sk": 1, "cc_name": "Witness Center"}],
                "date_dim": [calendar(key, text) for key, text in [(1, "1998-12-15"), (2, "1999-01-15"), (3, "1999-02-15")]],
                "catalog_sales": [sale("cs", key, cs_call_center_sk=1, cs_sales_price=amount)
                                  for key, amount in [(1, "100.00"), (2, "100.00"), (3, "300.00")]]}, "January1999 has lag/lead and sales100 versus yearly monthly mean200"
    if query_id == 58:
        return {"item": [item()], "date_dim": [calendar(1, "2000-01-03", d_week_seq=1)],
                "store_sales": [sale("ss")], "catalog_sales": [sale("cs")],
                "web_sales": [sale("ws")]}, "Original January3 week and equal positive three-channel revenues within all10percent bands"
    if query_id == 59:
        return {"store": [{"s_store_sk": 1, "s_store_id": "STORE1", "s_store_name": "Witness Store"}],
                "date_dim": [calendar(1, "2001-01-08", d_week_seq=1, d_day_name="Monday"),
                             calendar(2, "2002-01-07", d_week_seq=53, d_day_name="Monday")],
                "store_sales": [sale("ss", key, ss_store_sk=1, ss_sales_price="100.00") for key in [1, 2]]}, "Same store positive Monday sales in year windows and week indices52 apart"
    if query_id == 10:
        return {"customer": [customer(c_current_cdemo_sk=1)],
                "customer_address": [{"ca_address_sk": 1, "ca_county": "Rush County"}],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_gender": "F"}],
                "date_dim": [calendar(1, "2002-02-15")], "store_sales": [sale("ss")],
                "web_sales": [sale("ws")]}, "Rush County customer has store and web sales in original2002 month range"
    if query_id == 13:
        return {"store": [{"s_store_sk": 1}], "date_dim": [calendar(1, "2001-02-15")],
                "customer_demographics": [{"cd_demo_sk": 1, "cd_marital_status": "M", "cd_education_status": "Advanced Degree"}],
                "household_demographics": [{"hd_demo_sk": 1, "hd_dep_count": 3}],
                "customer_address": [{"ca_address_sk": 1, "ca_country": "United States", "ca_state": "TX"}],
                "store_sales": [sale("ss", ss_store_sk=1, ss_cdemo_sk=1, ss_hdemo_sk=1,
                                     ss_addr_sk=1, ss_sales_price="125.00", ss_net_profit="150.00")]}, "Original demographic, dependent-count, price, state and profit conjunction"
    if query_id == 16:
        return {"date_dim": [calendar(1, "2002-02-15")],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "GA"}],
                "call_center": [{"cc_call_center_sk": 1, "cc_county": "Williamson County"}],
                "catalog_sales": [sale("cs", cs_order_number=1, cs_warehouse_sk=warehouse,
                     cs_ship_date_sk=1, cs_ship_addr_sk=1, cs_call_center_sk=1,
                     cs_ext_ship_cost="1.00", cs_net_profit="10.00") for warehouse in [1, 2]]}, "One unreturned order shipped by two warehouses to original GA/county/date filters"
    if query_id == 19:
        return {"item": [item(i_manager_id=8, i_manufact_id=128)], "customer": [customer()],
                "customer_address": [{"ca_address_sk": 1, "ca_zip": "76232"}],
                "store": [{"s_store_sk": 1, "s_zip": "24128"}],
                "date_dim": [calendar(1, "1998-11-15")],
                "store_sales": [sale("ss", ss_store_sk=1)]}, "Manager8 November1998 sale with different customer/store ZIP prefixes"
    if query_id == 23:
        return {"item": [item()], "customer": [customer()],
                "date_dim": [calendar(1, "2000-02-15")],
                "store_sales": [sale("ss", ss_sales_price="10.00") for _ in range(5)],
                "catalog_sales": [sale("cs", cs_list_price="10.00")],
                "web_sales": [sale("ws", ws_list_price="20.00")]}, "Five same-item/date sales, best customer and original February cross-channel purchases"
    if query_id == 25:
        return {"item": [item()], "store": [{"s_store_sk": 1, "s_store_id": "STORE1", "s_store_name": "Witness Store"}],
                "date_dim": [calendar(1, "2001-04-15")],
                "store_sales": [sale("ss", ss_store_sk=1, ss_ticket_number=1, ss_net_profit="10.00")],
                "store_returns": [{"sr_item_sk": 1, "sr_customer_sk": 1, "sr_ticket_number": 1,
                                   "sr_returned_date_sk": 1, "sr_net_loss": "2.00"}],
                "catalog_sales": [sale("cs", cs_net_profit="3.00")]}, "Same customer/item/ticket return and catalog purchase within original April2001 windows"
    if query_id == 30:
        return {"customer": [customer(1), customer(2)],
                "customer_address": [{"ca_address_sk": 1, "ca_state": "GA"}],
                "date_dim": [calendar(1, "2002-02-15")],
                "web_returns": [{"wr_returning_customer_sk": key, "wr_returning_addr_sk": 1,
                     "wr_returned_date_sk": 1, "wr_return_amt": amount}
                     for key, amount in [(1, "100.00"), (2, "0.00")]]}, "Original GA return100 exceeds1.2 times same-state customer mean50"
    if query_id == 3:
        return {"item": [item(i_manufact_id=128)], "date_dim": [calendar(1, "2000-11-15")],
                "store_sales": [sale("ss")]}, "Manufacturer 128 November sale"
    if query_id == 8:
        return {"customer": [customer(key) for key in range(1, 12)],
                "customer_address": [{"ca_address_sk": 1, "ca_zip": "24128"}],
                "store": [{"s_store_sk": 1, "s_store_name": "Witness Store", "s_zip": "24128"}],
                "date_dim": [calendar(1, "1998-04-15")],
                "store_sales": [sale("ss", ss_store_sk=1, ss_net_profit="10.00")]}, "Eleven preferred customers in original ZIP list and matching store prefix"
    if query_id == 24:
        return {"item": [item(i_color="peach")], "customer": [customer(c_birth_country="CANADA")],
                "customer_address": [{"ca_address_sk": 1, "ca_zip": "24128", "ca_country": "United States"}],
                "store": [{"s_store_sk": 1, "s_store_name": "Witness Store", "s_zip": "24128", "s_market_id": 8}],
                "store_sales": [sale("ss", ss_ticket_number=1, ss_store_sk=1, ss_net_paid="100.00")],
                "store_returns": [{"sr_ticket_number": 1, "sr_item_sk": 1}]}, "Peach sale/return, matched ZIP, market8 and differing birth country"
    if query_id == 32:
        return {"item": [item(i_manufact_id=977)], "date_dim": [calendar(1, "2000-02-15")],
                "catalog_sales": [sale("cs", cs_ext_discount_amt="0.00"),
                                  sale("cs", cs_ext_discount_amt="100.00")]}, "Discount100 exceeds 1.3 times mean50"
    if query_id == 37:
        return {"item": [item(i_manufact_id=677)], "date_dim": [calendar(1, "2000-02-15")],
                "inventory": [{"inv_item_sk": 1, "inv_date_sk": 1, "inv_quantity_on_hand": 200}],
                "catalog_sales": [sale("cs")]}, "Original manufacturer, price75, quantity200 and date interval"
    if query_id == 38:
        return {"customer": [customer()], "date_dim": [calendar(1, "2000-01-15")],
                "store_sales": [sale("ss")], "catalog_sales": [sale("cs")],
                "web_sales": [sale("ws")]}, "Same customer and month-sequence1200 date in all three channels"
    if query_id == 39:
        dates = [calendar(index + 1, text) for index, text in enumerate([
            "2001-01-01", "2001-01-08", "2001-01-15", "2001-02-01", "2001-02-08", "2001-02-15"])]
        return {"item": [item()], "warehouse": [{"w_warehouse_sk": 1, "w_warehouse_name": "Witness Warehouse"}],
                "date_dim": dates, "inventory": [{"inv_item_sk": 1, "inv_warehouse_sk": 1,
                "inv_date_sk": index + 1, "inv_quantity_on_hand": value}
                for index, value in enumerate([0, 0, 100, 0, 0, 100])]}, "Same item/warehouse Jan+Feb sample coefficient sqrt3 exceeds1"
    if query_id == 41:
        return {"item": [item(i_manufact_id=738, i_category="Women", i_color="powder",
                              i_units="Ounce", i_size="medium")]}, "Original correlated manufacturer/category/color/unit/size combination"
    if query_id == 44:
        return {"item": [item(1), item(2)], "store_sales": [
            {"ss_item_sk": 1, "ss_store_sk": 4, "ss_addr_sk": 1, "ss_net_profit": "10.00"},
            {"ss_item_sk": 2, "ss_store_sk": 4, "ss_addr_sk": 1, "ss_net_profit": "20.00"},
            {"ss_item_sk": 1, "ss_store_sk": 4, "ss_addr_sk": None, "ss_net_profit": "5.00"}]}, "Store4 distinct profit groups above nonempty NULL-address reference"
    if query_id in (4, 11):
        tables = {"customer": [customer()], "date_dim": [calendar(1, "2001-01-15"), calendar(2, "2002-01-15")],
                  "store_sales": [sale("ss", 1, "100.00"), sale("ss", 2, "110.00")],
                  "web_sales": [sale("ws", 1, "100.00"), sale("ws", 2, "150.00")]}
        if query_id == 4:
            tables["catalog_sales"] = [sale("cs", 1, "100.00"), sale("cs", 2, "200.00")]
        return tables, "Same customer positive 2001/2002 totals; store1.1x, web1.5x, catalog2x"
    if query_id == 31:
        return {"customer_address": [{"ca_address_sk": 1, "ca_county": "Witness County"}],
                "date_dim": [calendar(index + 1, text) for index, text in enumerate(["2000-01-15", "2000-04-15", "2000-07-15"])],
                "store_sales": [sale("ss", index + 1, value, ss_addr_sk=1) for index, value in enumerate(["100.00", "110.00", "121.00"])],
                "web_sales": [sale("ws", index + 1, value, ws_bill_addr_sk=1) for index, value in enumerate(["100.00", "150.00", "225.00"])]}, "Same county three quarters; web1.5x growth exceeds store1.1x twice"
    raise ValueError(f"no reviewed witness recipe for query {query_id}")


def schema_columns(schema):
    tables = {}
    for name, body in re.findall(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", schema, re.S):
        columns = re.findall(r"(?m)^\s*(\w+)\s+(BIGINT|INTEGER|VARCHAR|DATE|DECIMAL\(\d+,\s*\d+\))", body)
        if not columns or name in tables:
            raise ValueError("invalid/duplicate source schema table")
        tables[name] = columns
    if len(tables) != 24:
        raise ValueError("expected all24 original table schemas")
    return tables


def typed_value(value, typ):
    if value is None:
        return NULL
    if typ in {"INTEGER", "BIGINT"}:
        bits = 32 if typ == "INTEGER" else 64
        if not isinstance(value, int) or isinstance(value, bool) or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            raise ValueError(f"invalid {typ} witness value: {value!r}")
        return str(value)
    if typ == "VARCHAR":
        if not isinstance(value, str) or value == NULL:
            raise ValueError("invalid/string NULL-marker witness value")
        return value
    if typ == "DATE":
        if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
            raise ValueError("invalid ISO date witness value")
        return value
    match = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", typ)
    if match:
        if not isinstance(value, str):
            raise ValueError("decimal witness values must be exact strings")
        precision, scale = map(int, match.groups())
        try:
            number = Decimal(value)
            scaled = number * (10 ** scale)
            if not number.is_finite() or scaled != scaled.to_integral_value() or abs(scaled) >= 10 ** precision:
                raise ValueError("decimal witness value exceeds original precision/scale")
        except InvalidOperation as error:
            raise ValueError("invalid decimal witness value") from error
        return format(number, f".{scale}f")
    raise ValueError(f"unsupported original column type: {typ}")


def checked_source(path):
    if digest(path) != FIXED_MANIFEST_SHA256:
        raise ValueError("fixed original manifest digest mismatch")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("suite") != "tpcds" or manifest.get("logical_queries") != 99:
        raise ValueError("expected original99 TPC-DS profile")
    schema = (path.parent / manifest["schema"]).resolve()
    if digest(schema) != manifest["schema_sha256"]:
        raise ValueError("original schema digest mismatch")
    for query in manifest["queries"]:
        for variant in query["variants"]:
            sql = (path.parent / variant["sql"]).resolve()
            if digest(sql) != variant["sha256"]:
                raise ValueError("original query SQL digest mismatch")
            variant["sql"] = str(sql)
    manifest["schema"] = str(schema)
    return manifest, schema_columns(schema.read_text(encoding="utf-8"))


def generate(source, output, query_id):
    output = output.resolve()
    if not output.is_relative_to(Path("/tmp")) or output == Path("/tmp"):
        raise ValueError("witness output must be a child directory of /tmp")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("witness output must be an empty directory")
    source = source.resolve()
    manifest, schemas = checked_source(source)
    rows, description = recipe(query_id)
    if set(rows) - set(schemas):
        raise ValueError("recipe contains unknown original table")
    encoded = {}
    for name, columns in schemas.items():
        known = {column for column, _ in columns}
        encoded[name] = []
        for row in rows.get(name, []):
            if set(row) - known:
                raise ValueError(f"recipe contains unknown columns in {name}: {set(row) - known}")
            encoded[name].append([typed_value(row.get(column), typ) for column, typ in columns])
    output.mkdir(parents=True, exist_ok=True)
    table_directory = output / "tables"
    table_directory.mkdir()
    tables = []
    for name, values in encoded.items():
        csv_path = table_directory / f"{name}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as stream:
            csv.writer(stream, delimiter="|", lineterminator="\n").writerows(values)
        tables.append({"name": name, "csv": str(csv_path.relative_to(output)), "rows": len(values), "sha256": digest(csv_path)})
    manifest.update({"query_manifest": str(source), "query_manifest_sha256": FIXED_MANIFEST_SHA256,
        "data_status": "directed_semantic_fixture_not_dsdgen", "data_source": "explicit_query_witness_recipe",
        "fixture_qualification": "query_directed_nonempty_change_semantics_only_not_standard_data_or_performance",
        "target_query_id": query_id, "witness_recipe_version": VERSION, "witness_description": description,
        "witness_generator_sha256": digest(Path(__file__)), "csv_path_base": "manifest_directory", "csv_null": NULL,
        "tables": tables, "scale_factor": None})
    path = output / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return {"manifest": str(path), "manifest_sha256": digest(path), "query_id": query_id,
            "input_rows": sum(len(values) for values in encoded.values()),
            "csv_bytes": sum((output / table["csv"]).stat().st_size for table in tables),
            "qualification": manifest["fixture_qualification"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--query", type=int, required=True)
    args = parser.parse_args()
    original = Path(__file__).resolve().parents[1] / "tpcds/manifest.json"
    print(json.dumps(generate(original, args.output, args.query)))
