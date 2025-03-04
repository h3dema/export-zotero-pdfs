# -*- coding: utf-8 -*-
"""

@@@  @@@  @@@@@@   @@@@@@@   @@@@@@@@  @@@@@@@@@@    @@@@@@
@@@  @@@  @@@@@@@  @@@@@@@@  @@@@@@@@  @@@@@@@@@@@  @@@@@@@@
@@!  @@@      @@@  @@!  @@@  @@!       @@! @@! @@!  @@!  @@@
!@!  @!@      @!@  !@!  @!@  !@!       !@! !@! !@!  !@!  @!@
@!@!@!@!  @!@!!@   @!@  !@!  @!!!:!    @!! !!@ @!@  @!@!@!@!
!!!@!!!!  !!@!@!   !@!  !!!  !!!!!:    !@!   ! !@!  !!!@!!!!
!!:  !!!      !!:  !!:  !!!  !!:       !!:     !!:  !!:  !!!
:!:  !:!      :!:  :!:  !:!  :!:       :!:     :!:  :!:  !:!
::   :::  :: ::::   :::: ::   :: ::::  :::     ::   ::   :::
 :   : :   : : :   :: :  :   : :: ::    :      :     :   : :


Copyright 2025 by Henrique Duarte Moura.
All rights reserved.

This file is part of export-zotero-pdfs,
and is released under the "MIT License Agreement".
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

Please see the LICENSE file that should have been included as part of this package.
"""
import argparse
import os
import sqlite3
import shutil
from tqdm import tqdm


# Ref. See schema at https://api.zotero.org/schema

ZOTERO_PATH = "/home/h3dema/henri/Zotero/"
FNAME = "zotero.sqlite"
OUTPUT_PATH = "./zotero_out"


def list_tables(cursor, verbose=False):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tableNames = []
    for row in sorted(cursor.fetchall()):
        if verbose:
            print(row)
        tableNames.append(row[0])
    if verbose:
        print()
    return tableNames

def list_table_description(cursor, table_name, verbose=False):
    """
    List table description
    """
    SQL = "PRAGMA table_info({})".format(table_name)
    cursor.execute(SQL)
    print("Table: {}".format(table_name))
    for row in sorted(cursor.fetchall()):
        print(row)

def get_items(cursor):
    """
    Get all items from the 'items' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM items")
    columns = [column[0] for column in cursor.description]
    items = []
    for row in cursor.fetchall():
        item = dict(zip(columns, row))
        items.append(item)
    return items

def get_item_types(cursor):
    """
    Get all item types from the 'itemTypes' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM itemTypes")
    columns = [column[0] for column in cursor.description]
    item_types = []
    for row in cursor.fetchall():
        item_type = dict(zip(columns, row))
        item_types.append(item_type)
    return item_types

def get_item_attachments(cursor):
    """
    Get all item attachments from the 'itemAttachments' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM itemAttachments")
    columns = [column[0] for column in cursor.description]
    item_attachments = []
    for row in cursor.fetchall():
        item_attachment = dict(zip(columns, row))
        item_attachments.append(item_attachment)
    return item_attachments

def get_item_data(cursor):
    """
    Get all item attachments from the 'itemAttachments' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM itemData")
    columns = [column[0] for column in cursor.description]
    item_attachments = []
    for row in cursor.fetchall():
        item_attachment = dict(zip(columns, row))
        item_attachments.append(item_attachment)
    return item_attachments


def get_item_data_values(cursor):
    """
    Get all item attachments from the 'itemAttachments' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM itemDataValues")
    columns = [column[0] for column in cursor.description]
    item_attachments = []
    for row in cursor.fetchall():
        item_attachment = dict(zip(columns, row))
        item_attachments.append(item_attachment)
    return item_attachments

def get_item_fields(cursor):
    """
    Get all item attachments from the 'itemAttachments' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM fields")
    columns = [column[0] for column in cursor.description]
    item_attachments = []
    for row in cursor.fetchall():
        item_attachment = dict(zip(columns, row))
        item_attachments.append(item_attachment)
    return item_attachments

def get_collections(cursor):
    """
    Get all item attachments from the 'itemAttachments' table and return as a list of dictionaries
    """
    cursor.execute("SELECT * FROM collections")
    columns = [column[0] for column in cursor.description]
    collections = []
    for row in cursor.fetchall():
        collection = dict(zip(columns, row))
        collection["items"] = []
        collections.append(collection)

    cursor.execute("SELECT * FROM collectionItems")
    columns = [column[0] for column in cursor.description]
    collectionItems = []
    for row in cursor.fetchall():
        item = dict(zip(columns, row))
        # find the right collection to insert
        collection = [x for x in collections if x["collectionID"] == item["collectionID"]]
        if len(collection) > 0:
            collection[0]["items"].append(item["itemID"])
        collectionItems.append(item)

    # dict_keys(['collectionID', 'collectionName', 'parentCollectionID', 'clientDateModified', 'libraryID', 'key', 'version', 'synced', 'items'])
    return collections


def combine_all_items(cursor, storate_path, verbose=False):
    items = get_items(cursor)
    if verbose:
        print("Number of items: {}".format(len(items)))
    item_types = get_item_types(cursor)
    item_attachments = get_item_attachments(cursor)
    item_data = get_item_data(cursor)
    item_data_values = get_item_data_values(cursor)
    item_data_values = get_item_data_values(cursor)
    item_fields = get_item_fields(cursor)

    all_files_in_zotero = dict()
    for item in items:
        itemID = item["itemID"]
        itemTypeID = item["itemTypeID"]
        selected_item_types = [x for x in item_types if x["itemTypeID"] == itemTypeID]
        selected_item_attachments = [x for x in item_attachments if x["itemID"] == itemID and 'pdf' in x['contentType']]
        selected_item_data = [x for x in item_data if x["itemID"] == itemID]

        for attachment in selected_item_attachments:
            if attachment is not None:
                if "path" in attachment and attachment["path"] is not None:
                    attachment["path"] = os.path.join(
                        storate_path,
                        item["key"],
                        attachment["path"].replace("storage:", "")
                    )
                    if os.path.exists(attachment["path"]):
                        attachment["file exists"] = os.path.exists(attachment["path"])
                # if parentItemID exists, then add the fields in item_data indexed by parentItemID to selected_item_data
                if attachment['parentItemID'] is not None:
                    selected_item_data.extend([x for x in item_data if x["itemID"] == attachment['parentItemID']])
                    if "parentItemID" not in item:
                        item["parentItemID"] = []
                    item["parentItemID"].append(attachment['parentItemID'])

        for data in selected_item_data:
            fieldID = data["fieldID"]
            field = [x for x in item_fields if x["fieldID"] == fieldID]
            if len(field) > 0:
                data["fieldName"] = field[0]["fieldName"]

            valueID = data["valueID"]
            value = [x for x in item_data_values if x["valueID"] == valueID]
            if len(value) > 0:
                data["value"] = value[0]["value"]
        item["attachments"] = selected_item_attachments[0] if len(selected_item_attachments) > 0 else dict()
        item["data"] = selected_item_data

        if len(selected_item_attachments) > 0:
            # print(item)
            # print(selected_item_types)
            all_files_in_zotero[itemID] = item
    return all_files_in_zotero


def clean_list(all_files_in_zotero):
    info = dict()
    for item in all_files_in_zotero.values():
        clean_item = {
            "itemID": item["itemID"],
            "pdf": 	item['attachments'].get('path'),
            "parentItemID": item.get("parentItemID", []),
            }
        for data in item["data"]:
            if data["fieldName"] == "title":
                clean_item["title"] = data["value"]
            else:
                pass  # nothing to do right now
        info[item["itemID"]] =clean_item

    return info


def update_with_info(collections, info, verbose=True):
    for collection in collections:
        selected_in_collection = []
        for itemID in collection["items"]:
            selected = [x for x in info.values() if x["itemID"] == itemID or itemID in x["parentItemID"]]
            if len(selected) > 0:
                selected_in_collection.append(selected[0])
        collection["selected_items"] = selected_in_collection
        if verbose:
            print("{:60s}: {}".format(collection["collectionName"], len(selected_in_collection)))

    return collections


def get_connection(zotero_path):
    """
    Establishes a connection to the Zotero database.

    Args:
        zotero_path (str): Path to the Zotero directory containing the database file.

    Returns:
        tuple: A tuple containing:
            - conn (sqlite3.Connection): Connection object to the Zotero SQLite database.
            - storate_path (str): Path to the Zotero storage directory.
    """

    storate_path = os.path.join(zotero_path, "storage")

    conn = sqlite3.connect(os.path.join(zotero_path, FNAME))
    return conn, storate_path


def menu():
    """
    Parses command line arguments for the application.

    Available options:
    --show-tables: Show tables in the database.
    --overwrite: Do not copy if the file already exists.
    --zotero-path: Path to Zotero storage directory (default: ZOTERO_PATH).
    --output-path: Path to output directory (default: OUTPUT_PATH).

    Returns:
        Namespace: Parsed command line arguments.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--show-tables", action="store_true", help="show tables in database")

    parser.add_argument("--overwrite", dest="overwrite", action="store_true", help="do not copy if file already exists")
    parser.add_argument("--no-overwrite", dest="overwrite", action="store_false", help="do not copy if file already exists")
    parser.set_defaults(overwrite=False)

    parser.add_argument("--zotero-path", type=str, default=ZOTERO_PATH, help="path to Zotero storage directory")
    parser.add_argument("--output-path", type=str, default=OUTPUT_PATH, help="path to output directory")
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    # -----------------------------
    #
    # read parameters
    #
    # -----------------------------
    args = menu()

    # -----------------------------
    #
    # Access database
    #
    # -----------------------------
    conn, storate_path = get_connection(args.zotero_path)
    cursor = conn.cursor()

    tableNames = list_tables(cursor)

    if args.show_tables:
        for table in tableNames:
            list_table_description(cursor, table)
            print()

    all_files_in_zotero = combine_all_items(cursor, storate_path)
    print("Entries in Zotero with local pdfs: {}".format(len(all_files_in_zotero)))

    # -----------------------------
    #
    # Get collections
    #
    # -----------------------------
    collections = get_collections(cursor)
    info = clean_list(all_files_in_zotero)

    collections = update_with_info(collections, info, verbose=False)

    # -----------------------------
    #
    # Export pdfs
    #
    # -----------------------------
    pbar = tqdm(total=sum([len(collection["selected_items"]) for collection in collections]))
    for collection in collections:
        for selected_item in collection["selected_items"]:
            # import pdb; pdb.set_trace()
            pdf_path = selected_item.get("pdf")
            pbar.update(1)
            if pdf_path and os.path.exists(pdf_path):
                destination_path = os.path.join(args.output_path, collection["collectionName"], os.path.basename(pdf_path))
                if os.path.exists(destination_path) and not args.overwrite:
                    continue
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                shutil.copy(pdf_path, destination_path)

    conn.close()
