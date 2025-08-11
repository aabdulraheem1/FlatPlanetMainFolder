"""
Calculate Model Performance Timer Management Command
Comprehensive timing analysis for calculate_model process
"""

import time
import psutil
import gc
from datetime import datetime
from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection
from website.models import (
    scenarios, SMART_Forecast_Model, CalculatedAggregatedForecast,
    CalcualtedReplenishmentModel, CalculatedProductionModel
)
from website.customized_function import populate_all_aggregated_data

class PerformanceTimer:
    def __init__(self):
        self.start_time = None
        self.steps = []
        self.process = psutil.Process()
        self.initial_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
    def start_step(self, step_name, description=""):
        if self.start_time is not None:
            self.end_step()
        
        self.start_time = time.time()
        self.current_step = {
            'name': step_name,
            'description': description,
            'start_time': self.start_time,
            'start_memory': self.process.memory_info().rss / 1024 / 1024
        }
        print(f"\n{'='*70}")
        print(f"🚀 STARTING: {step_name}")
        if description:
            print(f"📋 {description}")
        print(f"🕐 Start Time: {datetime.now().strftime('%H:%M:%S')}")
        print(f"💾 Memory: {self.current_step['start_memory']:.1f} MB")
        print(f"{'='*70}")
        
    def end_step(self):
        if self.start_time is None:
            return
            
        end_time = time.time()
        duration = end_time - self.start_time
        end_memory = self.process.memory_info().rss / 1024 / 1024
        memory_delta = end_memory - self.current_step['start_memory']
        
        self.current_step.update({
            'end_time': end_time,
            'duration': duration,
            'end_memory': end_memory,
            'memory_delta': memory_delta
        })
        
        self.steps.append(self.current_step)
        
        print(f"\n✅ COMPLETED: {self.current_step['name']}")
        print(f"⏱️  Duration: {duration/60:.2f} minutes ({duration:.1f} seconds)")
        print(f"💾 Memory Change: {memory_delta:+.1f} MB (now {end_memory:.1f} MB)")
        print(f"🕐 End Time: {datetime.now().strftime('%H:%M:%S')}")
        
        self.start_time = None
        self.current_step = None
        gc.collect()

class Command(BaseCommand):
    help = 'Comprehensive timing analysis for calculate_model process'

    def add_arguments(self, parser):
        parser.add_argument(
            'scenario_version',
            type=str,
            help="The scenario version to analyze (e.g., 'Jul 25 SPR')",
        )
        parser.add_argument(
            '--skip-cleanup',
            action='store_true',
            help="Skip initial data cleanup",
        )
        parser.add_argument(
            '--detailed',
            action='store_true',
            help="Show detailed database statistics",
        )

    def handle(self, *args, **options):
        scenario_version = options['scenario_version']
        skip_cleanup = options['skip_cleanup']
        detailed = options['detailed']
        
        timer = PerformanceTimer()
        overall_start = time.time()
        
        self.stdout.write(f"{'='*70}")
        self.stdout.write(f"🔍 CALCULATE MODEL PERFORMANCE ANALYSIS")
        self.stdout.write(f"{'='*70}")
        self.stdout.write(f"📊 Scenario: {scenario_version}")
        self.stdout.write(f"🕐 Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.stdout.write(f"💻 System: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total/(1024**3):.1f}GB RAM")
        
        try:
            scenario = scenarios.objects.get(version=scenario_version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario '{scenario_version}' not found"))
            return
        
        try:
            # Step 0: Initial Database Statistics
            timer.start_step("Initial Database Analysis", "Analyzing current database state")
            initial_stats = self.get_database_stats(scenario, detailed)
            self.display_database_stats("Initial", initial_stats)
            timer.end_step()
            
            # Step 1: Data Cleanup (optional)
            if not skip_cleanup:
                timer.start_step("Data Cleanup", "Removing existing calculated data")
                cleanup_stats = self.cleanup_existing_data(scenario)
                self.stdout.write(f"   🗑️  Cleaned: {cleanup_stats}")
                timer.end_step()
            
            # Step 2: Populate Aggregated Forecast
            timer.start_step("Populate Aggregated Forecast", "Command: populate_aggregated_forecast")
            try:
                call_command('populate_aggregated_forecast', scenario_version)
                self.stdout.write("   ✅ Aggregated forecast completed")
            except Exception as e:
                self.stdout.write(f"   ❌ Error: {str(e)}")
            timer.end_step()
            
            # Step 3: Populate Calculated Replenishment
            timer.start_step("Populate Calculated Replenishment", "Command: populate_calculated_replenishment_v2")
            try:
                call_command('populate_calculated_replenishment_v2', scenario_version)
                self.stdout.write("   ✅ Replenishment calculation completed")
            except Exception as e:
                self.stdout.write(f"   ❌ Error: {str(e)}")
            timer.end_step()
            
            # Step 4: Populate Calculated Production
            timer.start_step("Populate Calculated Production", "Command: populate_calculated_production")
            try:
                call_command('populate_calculated_production', scenario_version)
                self.stdout.write("   ✅ Production calculation completed")
            except Exception as e:
                self.stdout.write(f"   ❌ Error: {str(e)}")
            timer.end_step()
            
            # Step 5: Cache Review Data
            timer.start_step("Cache Review Data", "Command: cache_review_data")
            try:
                call_command('cache_review_data', scenario_version)
                self.stdout.write("   ✅ Cache review data completed")
            except Exception as e:
                self.stdout.write(f"   ❌ Error: {str(e)}")
            timer.end_step()
            
            # Step 6: Populate All Aggregated Data
            timer.start_step("Populate All Aggregated Data", "Function: populate_all_aggregated_data")
            try:
                result = populate_all_aggregated_data(scenario)
                self.stdout.write(f"   ✅ Aggregated data completed: {result}")
            except Exception as e:
                self.stdout.write(f"   ❌ Error: {str(e)}")
            timer.end_step()
            
            # Step 7: Final Database Statistics
            timer.start_step("Final Database Analysis", "Checking final database state")
            final_stats = self.get_database_stats(scenario, detailed)
            self.display_database_stats("Final", final_stats)
            timer.end_step()
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Critical Error: {str(e)}"))
            if timer.start_time is not None:
                timer.end_step()
        
        # Generate comprehensive summary
        overall_duration = time.time() - overall_start
        self.generate_performance_summary(timer.steps, overall_duration)
        self.analyze_bottlenecks(timer.steps, overall_duration)
        
    def get_database_stats(self, scenario, detailed=False):
        """Get database statistics for the scenario"""
        stats = {}
        
        try:
            with connection.cursor() as cursor:
                queries = [
                    ('Total Forecast Records', 'SELECT COUNT(*) FROM website_smart_forecast_model WHERE version_id = %s'),
                    ('Zero Qty Records', 'SELECT COUNT(*) FROM website_smart_forecast_model WHERE version_id = %s AND (Qty = 0 OR Qty IS NULL)'),
                    ('Non-Zero Qty Records', 'SELECT COUNT(*) FROM website_smart_forecast_model WHERE version_id = %s AND Qty > 0'),
                    ('Aggregated Forecast', 'SELECT COUNT(*) FROM website_calculatedaggregatedforecast WHERE version_id = %s'),
                    ('Replenishment Records', 'SELECT COUNT(*) FROM website_calcualtedreplenishmentmodel WHERE version_id = %s'),
                    ('Production Records', 'SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = %s'),
                ]
                
                if detailed:
                    queries.extend([
                        ('Inventory Records', 'SELECT COUNT(*) FROM website_masterdatainventory WHERE version_id = %s'),
                        ('Product Cost Records', 'SELECT COUNT(*) FROM website_productsitecostmodel WHERE version_id = %s'),
                    ])
                
                for name, query in queries:
                    cursor.execute(query, [scenario.id])
                    stats[name] = cursor.fetchone()[0]
                    
        except Exception as e:
            stats['error'] = str(e)
            
        return stats
    
    def display_database_stats(self, label, stats):
        """Display database statistics in a formatted way"""
        self.stdout.write(f"\n📊 {label} Database Statistics:")
        if 'error' in stats:
            self.stdout.write(f"   ❌ Error: {stats['error']}")
        else:
            for key, value in stats.items():
                if key != 'error':
                    self.stdout.write(f"   {key}: {value:,}")
    
    def cleanup_existing_data(self, scenario):
        """Clean up existing calculated data and return statistics"""
        try:
            agg_deleted = CalculatedAggregatedForecast.objects.filter(version=scenario).delete()[0]
            rep_deleted = CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()[0]
            prod_deleted = CalculatedProductionModel.objects.filter(version=scenario).delete()[0]
            
            return f"{agg_deleted:,} aggregated, {rep_deleted:,} replenishment, {prod_deleted:,} production"
            
        except Exception as e:
            return f"Error: {str(e)}"
    
    def generate_performance_summary(self, steps, total_duration):
        """Generate and display performance summary"""
        self.stdout.write(f"\n{'='*70}")
        self.stdout.write(f"📈 PERFORMANCE SUMMARY")
        self.stdout.write(f"{'='*70}")
        
        self.stdout.write(f"\n⏱️  DETAILED TIMING BREAKDOWN:")
        self.stdout.write(f"{'Step':<35} {'Duration':<12} {'% Total':<10} {'Memory Δ':<12}")
        self.stdout.write(f"{'-'*75}")
        
        for step in steps:
            duration_pct = (step['duration'] / total_duration) * 100
            duration_str = f"{step['duration']/60:.2f}m"
            step_name = step['name'][:33]
            memory_delta = step.get('memory_delta', 0)
            
            self.stdout.write(f"{step_name:<35} {duration_str:<12} {duration_pct:.1f}% {memory_delta:+.1f}MB")
        
        self.stdout.write(f"{'-'*75}")
        self.stdout.write(f"{'TOTAL':<35} {total_duration/60:.2f}m 100.0%")
        
        # Calculate additional metrics
        if steps:
            avg_step_time = total_duration / len(steps)
            max_memory_step = max(steps, key=lambda x: x.get('end_memory', 0))
            total_memory_growth = sum(step.get('memory_delta', 0) for step in steps)
            
            self.stdout.write(f"\n🎯 KEY METRICS:")
            self.stdout.write(f"   ⏱️  Total Processing Time: {total_duration/60:.2f} minutes")
            self.stdout.write(f"   📊 Number of Steps: {len(steps)}")
            self.stdout.write(f"   ⚡ Average Step Time: {avg_step_time/60:.2f} minutes")
            self.stdout.write(f"   🧠 Total Memory Growth: {total_memory_growth:.1f} MB")
            self.stdout.write(f"   🔝 Peak Memory Step: {max_memory_step['name']} ({max_memory_step.get('end_memory', 0):.1f} MB)")
    
    def analyze_bottlenecks(self, steps, total_duration):
        """Analyze performance bottlenecks and provide recommendations"""
        
        self.stdout.write(f"\n{'='*70}")
        self.stdout.write(f"🔍 BOTTLENECK ANALYSIS & OPTIMIZATION RECOMMENDATIONS")
        self.stdout.write(f"{'='*70}")
        
        # Sort steps by duration to identify bottlenecks
        sorted_steps = sorted(steps, key=lambda x: x['duration'], reverse=True)
        
        self.stdout.write(f"\n🎯 TOP 3 BOTTLENECKS:")
        for i, step in enumerate(sorted_steps[:3], 1):
            duration_minutes = step['duration'] / 60
            duration_pct = (step['duration'] / total_duration) * 100
            
            self.stdout.write(f"\n{i}. {step['name']} - {duration_minutes:.2f} minutes ({duration_pct:.1f}% of total)")
            
            # Provide specific recommendations based on step name
            recommendations = self.get_optimization_recommendations(step['name'], duration_minutes)
            for rec in recommendations:
                self.stdout.write(f"   💡 {rec}")
        
        # Overall recommendations
        self.stdout.write(f"\n🚀 GENERAL OPTIMIZATION STRATEGIES:")
        if total_duration > 10 * 60:  # More than 10 minutes
            self.stdout.write("   🔴 CRITICAL: Process taking > 10 minutes")
            self.stdout.write("   💡 Consider running cleanup_zero_records first")
            self.stdout.write("   💡 Use optimized_calculate_model with --parallel flag")
            self.stdout.write("   💡 Increase database connection pool size")
        elif total_duration > 5 * 60:  # More than 5 minutes
            self.stdout.write("   🟡 MODERATE: Process taking > 5 minutes")
            self.stdout.write("   💡 Consider batch size optimization")
            self.stdout.write("   💡 Check database indexes")
        else:
            self.stdout.write("   🟢 GOOD: Process completing in reasonable time")
            self.stdout.write("   💡 Monitor for performance regression")
        
        self.stdout.write(f"\n✅ ANALYSIS COMPLETE")
        self.stdout.write(f"📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def get_optimization_recommendations(self, step_name, duration_minutes):
        """Get specific optimization recommendations for a step"""
        recommendations = []
        
        step_lower = step_name.lower()
        
        if 'replenishment' in step_lower:
            recommendations.extend([
                "Use larger batch sizes (--batch-size 5000+)",
                "Add database indexes on Product_id and Site_id",
                "Filter zero quantities before processing",
                "Consider parallel processing for large datasets"
            ])
        elif 'aggregated' in step_lower:
            recommendations.extend([
                "Use SQL aggregation instead of Python loops",
                "Filter zero records at database level",
                "Create materialized views for complex aggregations",
                "Optimize GROUP BY queries with proper indexes"
            ])
        elif 'production' in step_lower:
            recommendations.extend([
                "Pre-load cost data into memory",
                "Use polars for inventory calculations",
                "Optimize joins with sorted data",
                "Batch inventory deduction operations"
            ])
        elif 'cache' in step_lower:
            recommendations.extend([
                "Use async processing for cache generation",
                "Implement incremental cache updates",
                "Consider Redis for faster caching",
                "Cache intermediate calculation results"
            ])
        elif 'cleanup' in step_lower:
            recommendations.extend([
                "Use bulk delete operations",
                "Disable foreign key checks during cleanup",
                "Consider truncate for full table clears"
            ])
        else:
            recommendations.extend([
                "Profile this step for specific bottlenecks",
                "Consider breaking into smaller sub-steps",
                "Monitor memory usage during execution"
            ])
        
        return recommendations
