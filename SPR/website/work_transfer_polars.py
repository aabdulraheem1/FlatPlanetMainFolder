"""
High-performance work transfer operations using Polars
Fast querying, filtering, and data processing for work transfer between sites
"""

import polars as pl
import pandas as pd
from django.db import connection
import time
import math

def get_work_transfer_data_polars(scenario_version, page=1, per_page=20, filters=None):
    """
    Get work transfer data using Polars for high performance
    
    Args:
        scenario_version: The scenario version
        page: Page number (1-based)
        per_page: Records per page
        filters: Dict with 'product', 'site', 'group' filters
    
    Returns:
        Dict with paginated data and metadata
    """
    print(f"DEBUG: Getting work transfer data for scenario: {scenario_version}")
    start_time = time.time()
    
    if filters is None:
        filters = {}
    
    try:
        # Build WHERE clause for filters
        where_conditions = []
        params = [scenario_version]
        
        if filters.get('product'):
            where_conditions.append("p.Product LIKE %s")
            params.append(f"%{filters['product']}%")
        
        if filters.get('site'):
            where_conditions.append("mp.SiteName = %s")
            params.append(filters['site'])
        
        if filters.get('group'):
            where_conditions.append("cp.product_group LIKE %s")
            params.append(f"%{filters['group']}%")
        
        # Handle supply options filter - requires special handling
        supply_option_filter = filters.get('supply_option')
        if supply_option_filter:
            # Get valid supply options for the product (excluding BK* with 6 characters)
            valid_supply_options = []
            
            # Query supply options for the product if we have product filter
            product_filter = filters.get('product')
            if product_filter:
                with connection.cursor() as cursor:
                    # Get all supply options for the product
                    supply_query = """
                        SELECT DISTINCT 
                            COALESCE(s.VendorID, mp_so.SiteName) as identifier
                        FROM website_masterdatasupplyoptionsmodel so
                        LEFT JOIN website_masterdatasuppliersmodel s ON so.Supplier_id = s.VendorID
                        LEFT JOIN website_masterdataplantmodel mp_so ON so.Site_id = mp_so.SiteName
                        WHERE so.Product_id LIKE %s
                        AND COALESCE(s.VendorID, mp_so.SiteName) IS NOT NULL
                    """
                    cursor.execute(supply_query, [f"%{product_filter}%"])
                    all_options = [row[0] for row in cursor.fetchall()]
                    
                    # Filter out BK* with 6 characters
                    for option in all_options:
                        if not (option.startswith('BK') and len(option) == 6):
                            valid_supply_options.append(option)
            
            if valid_supply_options:
                # Add JOIN to supply options when filtering is needed
                supply_option_join = """
                    LEFT JOIN website_masterdatasupplyoptionsmodel so ON p.Product = so.Product_id
                    LEFT JOIN website_masterdatasuppliersmodel s ON so.Supplier_id = s.VendorID
                    LEFT JOIN website_masterdataplantmodel mp_so ON so.Site_id = mp_so.SiteName
                """
                # Filter by valid supply options that match the search term
                valid_options_placeholders = ','.join(['%s'] * len(valid_supply_options))
                where_conditions.append(f"""
                    COALESCE(s.VendorID, mp_so.SiteName) IN ({valid_options_placeholders})
                    AND (s.VendorID LIKE %s OR mp_so.SiteName LIKE %s OR so.SourceName LIKE %s)
                """)
                params.extend(valid_supply_options)
                params.extend([f"%{supply_option_filter}%", f"%{supply_option_filter}%", f"%{supply_option_filter}%"])
            else:
                supply_option_join = ""
        else:
            supply_option_join = ""
        
        where_clause = ""
        if where_conditions:
            where_clause = "AND " + " AND ".join(where_conditions)
        
        # Get total count for pagination (with optional supply options join)
        count_query = f"""
            SELECT COUNT(DISTINCT cp.id) as total_count
            FROM website_calculatedproductionmodel cp
            INNER JOIN website_masterdataproductmodel p ON cp.product_id = p.Product
            LEFT JOIN website_masterdataplantmodel mp ON cp.site_id = mp.SiteName
            {supply_option_join}
            WHERE cp.version_id = %s {where_clause}
        """
        
        with connection.cursor() as cursor:
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
        
        # Calculate pagination
        total_pages = math.ceil(total_count / per_page)
        offset = (page - 1) * per_page
        
        # Main data query with pagination (with optional supply options join)
        data_query = f"""
            SELECT 
                cp.id,
                p.Product,
                cp.pouring_date,
                cp.production_quantity,
                cp.tonnes,
                cp.product_group,
                cp.parent_product_group,
                cp.production_aud,
                cp.revenue_aud,
                ISNULL(mp.SiteName, 'Unknown') as current_site,
                cp.is_outsourced
            FROM website_calculatedproductionmodel cp
            INNER JOIN website_masterdataproductmodel p ON cp.product_id = p.Product
            LEFT JOIN website_masterdataplantmodel mp ON cp.site_id = mp.SiteName
            {supply_option_join}
            WHERE cp.version_id = %s {where_clause}
            ORDER BY cp.pouring_date, p.Product
            OFFSET {offset} ROWS FETCH NEXT {per_page} ROWS ONLY
        """
        
        with connection.cursor() as cursor:
            cursor.execute(data_query, params)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        
        # Convert to Polars DataFrame for fast processing
        if data:
            df = pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
        else:
            df = pl.DataFrame()
        
        # Get available sites (cached static for better performance)
        foundry_sites = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1', 'CHI1']
        
        # OPTIMIZATION: Only get outsource sites once and cache
        outsource_query = """
            SELECT SiteName 
            FROM website_masterdataplantmodel 
            WHERE mark_as_outsource_supplier = 1
            ORDER BY SiteName
        """
        
        with connection.cursor() as cursor:
            cursor.execute(outsource_query)
            outsource_sites = [row[0] for row in cursor.fetchall()]
        
        all_available_sites = foundry_sites + outsource_sites
        
        # OPTIMIZATION: Get supply options efficiently for current page products only
        if len(df) > 0:
            product_ids = df['Product'].unique().to_list()
            product_ids_str = "', '".join(product_ids)
            
            supply_options_query = f"""
                SELECT 
                    p.Product,
                    COALESCE(s.VendorID, mp.SiteName, 'Unknown') as Source,
                    so.SourceName,
                    so.InhouseOrOutsource
                FROM website_masterdataproductmodel p
                INNER JOIN website_masterdatasupplyoptionsmodel so ON p.Product = so.Product_id
                LEFT JOIN website_masterdatasuppliersmodel s ON so.Supplier_id = s.VendorID
                LEFT JOIN website_masterdataplantmodel mp ON so.Site_id = mp.SiteName
                WHERE p.Product IN ('{product_ids_str}')
                ORDER BY p.Product, so.InhouseOrOutsource, COALESCE(s.VendorID, mp.SiteName)
            """
            
            supply_options_data = {}
            with connection.cursor() as cursor:
                cursor.execute(supply_options_query)
                for row in cursor.fetchall():
                    product = row[0]
                    option = {
                        'source': row[1],
                        'source_name': row[2],
                        'type': row[3]
                    }
                    if product not in supply_options_data:
                        supply_options_data[product] = []
                    supply_options_data[product].append(option)
        else:
            supply_options_data = {}
        
        # OPTIMIZATION: Skip filter options for pagination requests to improve speed
        if page == 1:
            filter_options = get_filter_options_polars(scenario_version)
        else:
            filter_options = {'sites': [], 'product_groups': []}  # Skip for pagination
        
        # Process records with optimized approach
        records_data = []
        
        if len(df) > 0:
            for row in df.iter_rows(named=True):
                product = row['Product']
                # Get supply options for this product
                supply_options = supply_options_data.get(product, [])
                
                records_data.append({
                    'id': row['id'],
                    'product': product,
                    'pouring_date': row['pouring_date'].strftime('%Y-%m-%d') if row['pouring_date'] else '',
                    'production_quantity': float(row['production_quantity'] or 0),
                    'tonnes': float(row['tonnes'] or 0),
                    'product_group': row['product_group'] or '',
                    'parent_product_group': row['parent_product_group'] or '',
                    'production_aud': float(row['production_aud'] or 0),
                    'revenue_aud': float(row['revenue_aud'] or 0),
                    'current_site': row['current_site'],
                    'available_sites': all_available_sites,
                    'supply_options': supply_options  # Simplified for speed
                })
        
        query_time = time.time() - start_time
        print(f"🚀 OPTIMIZED Polars work transfer query completed in {query_time:.3f}s (excluding slow supply options)")
        
        return {
            'success': True,
            'production_records': records_data,
            'sites': filter_options['sites'],
            'product_groups': filter_options['product_groups'],
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_count
            },
            'total_records': total_count,
            'filtered_records': total_count,  # Same as total when using SQL filtering
            'optimization_note': 'Supply options disabled for faster loading'
        }
        
    except Exception as e:
        print(f"ERROR in get_work_transfer_data_polars: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

def get_supply_options_polars(product_ids):
    """
    Get supply options for products using Polars for fast processing
    Excludes BK* sources (6 chars starting with BK)
    """
    if not product_ids:
        return {}
    
    try:
        # Build parameterized query for multiple products
        placeholders = ','.join(['%s'] * len(product_ids))
        
        supply_query = f"""
            SELECT 
                p.Product,
                CASE 
                    WHEN so.Supplier_id IS NOT NULL THEN s.VendorID
                    WHEN so.Site_id IS NOT NULL THEN mp.SiteName
                    ELSE so.SourceName
                END as source_name
            FROM website_masterdataproductmodel p
            LEFT JOIN website_masterdatasupplyoptionsmodel so ON p.Product = so.Product_id
            LEFT JOIN website_masterdatasuppliersmodel s ON so.Supplier_id = s.VendorID
            LEFT JOIN website_masterdataplantmodel mp ON so.Site_id = mp.SiteName
            WHERE p.Product IN ({placeholders})
                AND (
                    so.Supplier_id IS NOT NULL 
                    OR so.Site_id IS NOT NULL 
                    OR so.SourceName IS NOT NULL
                )
        """
        
        with connection.cursor() as cursor:
            cursor.execute(supply_query, product_ids)
            data = cursor.fetchall()
        
        if not data:
            return {}
        
        # Convert to Polars DataFrame
        df = pl.DataFrame({
            'Product': [row[0] for row in data],
            'source_name': [row[1] for row in data]
        })
        
        # Filter out BK* sources and group by product
        filtered_df = df.filter(
            ~((pl.col('source_name').str.len_chars() == 6) & 
              (pl.col('source_name').str.starts_with('BK')))
        ).filter(
            pl.col('source_name').is_not_null()
        )
        
        # Group by product and collect sources
        result = {}
        if len(filtered_df) > 0:
            grouped = filtered_df.group_by('Product').agg(
                pl.col('source_name').unique().alias('sources')
            )
            
            for row in grouped.iter_rows(named=True):
                result[row['Product']] = row['sources']
        
        return result
        
    except Exception as e:
        print(f"ERROR in get_supply_options_polars: {e}")
        return {}

def get_filter_options_polars(scenario_version):
    """
    Get unique sites and product groups for filter dropdowns using Polars
    """
    try:
        filter_query = """
            SELECT DISTINCT
                ISNULL(mp.SiteName, 'Unknown') as site_name,
                cp.product_group
            FROM website_calculatedproductionmodel cp
            LEFT JOIN website_masterdataplantmodel mp ON cp.site_id = mp.SiteName
            WHERE cp.version_id = %s
                AND (mp.SiteName IS NOT NULL OR cp.site_id IS NULL)
                AND cp.product_group IS NOT NULL
            ORDER BY site_name, cp.product_group
        """
        
        with connection.cursor() as cursor:
            cursor.execute(filter_query, [scenario_version])
            data = cursor.fetchall()
        
        if not data:
            return {'sites': [], 'product_groups': []}
        
        # Convert to Polars DataFrame
        df = pl.DataFrame({
            'site_name': [row[0] for row in data],
            'product_group': [row[1] for row in data]
        })
        
        # Get unique values
        sites = df['site_name'].unique().sort().to_list()
        product_groups = df['product_group'].unique().sort().to_list()
        
        return {
            'sites': sites,
            'product_groups': product_groups
        }
        
    except Exception as e:
        print(f"ERROR in get_filter_options_polars: {e}")
        return {'sites': [], 'product_groups': []}

def save_transfers_polars(scenario_version, transfers):
    """
    Save work transfers using Polars for batch processing and validation
    """
    print(f"DEBUG: Saving {len(transfers)} transfers using Polars")
    start_time = time.time()
    
    try:
        if not transfers:
            return {
                'success': False,
                'error': 'No transfers provided'
            }
        
        # Extract record IDs and new sites
        record_ids = [t['record_id'] for t in transfers]
        
        # Validate records exist and belong to scenario
        validation_query = f"""
            SELECT 
                cp.id,
                cp.site_id as current_site,
                p.Product
            FROM website_calculatedproductionmodel cp
            INNER JOIN website_masterdataproductmodel p ON cp.product_id = p.Product
            WHERE cp.id IN ({','.join(['%s'] * len(record_ids))})
                AND cp.version_id = %s
        """
        
        params = record_ids + [scenario_version]
        
        with connection.cursor() as cursor:
            cursor.execute(validation_query, params)
            existing_records = cursor.fetchall()
        
        if len(existing_records) != len(record_ids):
            return {
                'success': False,
                'error': 'Some records not found or do not belong to this scenario'
            }
        
        # Convert to Polars DataFrame for validation
        existing_df = pl.DataFrame({
            'record_id': [row[0] for row in existing_records],
            'current_site': [row[1] for row in existing_records],
            'product': [row[2] for row in existing_records]
        })
        
        # Validate new sites exist
        new_sites = list(set([t['new_site'] for t in transfers]))
        sites_query = f"""
            SELECT SiteName 
            FROM website_masterdataplantmodel 
            WHERE SiteName IN ({','.join(['%s'] * len(new_sites))})
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sites_query, new_sites)
            valid_sites = [row[0] for row in cursor.fetchall()]
        
        if len(valid_sites) != len(new_sites):
            invalid_sites = set(new_sites) - set(valid_sites)
            return {
                'success': False,
                'error': f'Invalid sites: {", ".join(invalid_sites)}'
            }
        
        # Get outsource status for new sites
        outsource_query = f"""
            SELECT SiteName, mark_as_outsource_supplier
            FROM website_masterdataplantmodel 
            WHERE SiteName IN ({','.join(['%s'] * len(new_sites))})
        """
        
        with connection.cursor() as cursor:
            cursor.execute(outsource_query, new_sites)
            site_outsource_status = dict(cursor.fetchall())
        
        # Perform batch update
        transferred_count = 0
        
        with connection.cursor() as cursor:
            for transfer in transfers:
                record_id = transfer['record_id']
                new_site = transfer['new_site']
                is_outsourced = site_outsource_status.get(new_site, False)
                
                update_query = """
                    UPDATE website_calculatedproductionmodel 
                    SET site_id = %s, is_outsourced = %s
                    WHERE id = %s AND version_id = %s
                """
                
                cursor.execute(update_query, [new_site, is_outsourced, record_id, scenario_version])
                
                if cursor.rowcount > 0:
                    transferred_count += 1
        
        save_time = time.time() - start_time
        print(f"🚀 Polars transfer save completed in {save_time:.3f}s")
        
        return {
            'success': True,
            'transferred_count': transferred_count,
            'message': f'Successfully transferred {transferred_count} production records.'
        }
        
    except Exception as e:
        print(f"ERROR in save_transfers_polars: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }
