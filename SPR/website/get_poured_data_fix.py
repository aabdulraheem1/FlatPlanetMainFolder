def get_monthly_poured_data_for_site_and_fy(site, fy, scenario_version):
    """
    OPTIMIZED: Get monthly poured data from local MonthlyPouredDataModel (FAST!)
    Data is populated locally when inventory snapshot is uploaded via upload_on_hand_stock
    This replaces slow PowerBI database queries with fast local database access
    """
    try:
        from website.models import MonthlyPouredDataModel, scenarios
        from urllib.parse import unquote
        
        # URL decode the scenario version in case it comes from a URL
        decoded_scenario_version = unquote(scenario_version) if isinstance(scenario_version, str) else scenario_version
        
        print(f"📋 Reading monthly poured data from local database for {site} {fy} (optimized)...")
        print(f"📋 Original scenario version: '{scenario_version}'")
        print(f"📋 Decoded scenario version: '{decoded_scenario_version}'")
        
        # Try to get scenario with decoded version first
        try:
            scenario = scenarios.objects.get(version=decoded_scenario_version)
            print(f"✅ Found scenario using decoded version: {scenario}")
        except scenarios.DoesNotExist:
            print(f"❌ Decoded version '{decoded_scenario_version}' not found, trying original...")
            # Fallback to original version
            scenario = scenarios.objects.get(version=scenario_version)
        
        print(f"📋 Found scenario object: {scenario}")
        
        # Check if we have any MonthlyPouredDataModel records at all
        total_records = MonthlyPouredDataModel.objects.filter(version=scenario).count()
        print(f"📋 Total MonthlyPouredDataModel records for scenario: {total_records}")
        
        # Check for specific site and FY
        site_fy_records = MonthlyPouredDataModel.objects.filter(
            version=scenario, 
            site_name=site, 
            fiscal_year=fy
        ).count()
        print(f"📋 Records for {site} {fy}: {site_fy_records}")
        
        # Fast local database query
        monthly_data = MonthlyPouredDataModel.get_monthly_data_for_site_and_fy(
            scenario=scenario,
            site=site,
            fy=fy
        )
        
        if monthly_data:
            print(f"✅ Retrieved {len(monthly_data)} months of data for {site} {fy} from local database")
            for month, tonnes in monthly_data.items():
                print(f"   {month}: {tonnes} tonnes")
        else:
            print(f"⚠️  No monthly poured data found for {site} {fy} in local database")
            print("   💡 Make sure inventory snapshot was uploaded via upload_on_hand_stock")
            
            # Add debug info about what records exist
            all_records = MonthlyPouredDataModel.objects.filter(version=scenario)
            if all_records.exists():
                print(f"   📊 Available data in database:")
                sites_available = all_records.values_list('site_name', flat=True).distinct()
                fys_available = all_records.values_list('fiscal_year', flat=True).distinct()
                print(f"      Sites: {list(sites_available)}")
                print(f"      Fiscal Years: {list(fys_available)}")
            else:
                print("   📊 No MonthlyPouredDataModel records found for this scenario")
        
        return monthly_data
        
    except scenarios.DoesNotExist:
        print(f"❌ Scenario '{decoded_scenario_version}' (and '{scenario_version}') not found")
        
        # Debug: Show what scenarios ARE available
        all_scenarios = scenarios.objects.all()
        print(f"📊 Available scenarios in database:")
        for s in all_scenarios:
            print(f"   - '{s.version}' (ID: {s.id})")
        
        # Check for similar scenarios
        similar_scenarios = scenarios.objects.filter(version__icontains="Jul").values_list('version', flat=True)
        print(f"📊 Scenarios containing 'Jul': {list(similar_scenarios)}")
        
        return {}
    except Exception as e:
        print(f"❌ Error reading monthly poured data from local database: {str(e)}")
        return {}
