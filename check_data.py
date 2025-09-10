#!/usr/bin/env python3
"""Quick data check script"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from oracle_import_tool.config.config_manager import ConfigManager
from oracle_import_tool.database.connection import DatabaseConnection

def main():
    # Initialize config and connection
    config_manager = ConfigManager('config.ini')
    db_connection = DatabaseConnection(config_manager)
    
    try:
        db_connection.connect()
        print("SUCCESS: Database connected successfully")
        
        # Check TO_PICKING_SMT table
        print("\nCHECKING: TO_PICKING_SMT table...")
        
        # Get total count
        cursor = db_connection.cursor
        cursor.execute("SELECT COUNT(*) FROM TO_PICKING_SMT")
        total_count = cursor.fetchone()[0]
        print(f"TOTAL RECORDS in TO_PICKING_SMT: {total_count}")
        
        if total_count > 0:
            # Get sample data
            cursor.execute("SELECT * FROM TO_PICKING_SMT WHERE ROWNUM <= 5")
            columns = [desc[0] for desc in cursor.description]
            print(f"\nCOLUMNS: {', '.join(columns)}")
            print("\nSAMPLE RECORDS:")
            for i, row in enumerate(cursor.fetchall()):
                print(f"  Record {i+1}: {dict(zip(columns, row))}")
            
            # Check for specific ORDER_NO patterns
            cursor.execute("SELECT DISTINCT ORDER_NO FROM TO_PICKING_SMT WHERE ROWNUM <= 10")
            order_nos = [row[0] for row in cursor.fetchall()]
            print(f"\nSAMPLE ORDER_NO values: {order_nos}")
            
            # Check if the specific ORDER_NO exists
            specific_order = '0014812170'
            cursor.execute("SELECT COUNT(*) FROM TO_PICKING_SMT WHERE ORDER_NO = :1", (specific_order,))
            specific_count = cursor.fetchone()[0]
            print(f"\nSPECIFIC SEARCH - Records with ORDER_NO = '{specific_order}': {specific_count}")
            
        else:
            print("ERROR: Table is empty!")
        
        # Check other tables too
        for table_name in ['TM_ROLL', 'TM_ROLL_MNG']:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"TOTAL RECORDS in {table_name}: {count}")
            except Exception as e:
                print(f"ERROR checking {table_name}: {e}")
            
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        db_connection.disconnect()

if __name__ == '__main__':
    main()