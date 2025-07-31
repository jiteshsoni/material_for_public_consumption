#!/usr/bin/env python3
"""
Simplified Lakebase High-Concurrency Benchmark
"""

import json
import time
import random
import psycopg2
import statistics
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from lakebase_sdk_auth import LakebaseSDKAuthManager
from enhanced_production_pool import EnhancedProductionConnectionPool

class LakebaseBenchmark:
    def __init__(self):
        """Initialize benchmark with authentication and load config"""
        self.auth_manager = LakebaseSDKAuthManager()
        self.results = []
        self.connection_pools = {}  # Pool per thread count to avoid contention
        
        # Load benchmark configuration from file
        try:
            # Use secure config finder
            from secure_config_finder import find_config_file
            config_file = find_config_file()

            with open(config_file, 'r') as f:
                config = json.load(f)
                benchmark_config = config.get('benchmark', {})
                
            self.thread_counts = benchmark_config.get('thread_counts', [64, 128, 256])
            self.test_duration = benchmark_config.get('test_duration_seconds', 60)
            self.table_rows = benchmark_config.get('table_rows', 1000000)
            self.batch_size = benchmark_config.get('batch_size', 10000)
            self.test_table = benchmark_config.get('table_name', 'benchmark_test')
            self.connection_stagger_delay = benchmark_config.get('connection_stagger_delay', 0.1)
            self.max_pool_init_time = benchmark_config.get('max_pool_init_time', 30)
            self.use_connection_pooling = benchmark_config.get('use_connection_pooling', True)
            
            print(f"📋 Configuration:")
            print(f"   Table name: {self.test_table}")
            print(f"   Target rows: {self.table_rows:,}")
            print(f"   Thread counts: {self.thread_counts}")
            print(f"   Test duration: {self.test_duration}s per test")
            print(f"   Connection pooling: {'Enabled' if self.use_connection_pooling else 'Disabled'}")
            if self.use_connection_pooling:
                print(f"   Connection stagger delay: {self.connection_stagger_delay}s")
                print(f"   Max pool init time: {self.max_pool_init_time}s")
            
        except Exception as e:
            print(f"⚠️  Using defaults: {e}")
            self.thread_counts = [64, 128, 256]
            self.test_duration = 60
            self.table_rows = 1000000
            self.batch_size = 10000
            self.test_table = 'benchmark_test'
            self.connection_stagger_delay = 0.1
            self.max_pool_init_time = 30
            self.use_connection_pooling = True
        
    def get_connection(self, quiet=False):
        """Get database connection"""
        return psycopg2.connect(**self.auth_manager.get_connection_params(quiet=quiet))
    
    def display_table_info(self):
        """Display current table information"""
        print(f"📋 Table Information:")
        print(f"   📊 Table Name: {self.test_table}")
        print(f"   🎯 Target Rows: {self.table_rows:,}")
        
        try:
            conn = self.get_connection(quiet=True)
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{self.test_table}';
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                cursor.execute(f"SELECT COUNT(*) FROM {self.test_table};")
                current_rows = cursor.fetchone()[0]
                print(f"   ✅ Current Rows: {current_rows:,}")
                
                if current_rows == self.table_rows:
                    print(f"   🎯 Table ready for benchmark!")
                else:
                    print(f"   ⚠️  Table needs setup (current: {current_rows:,}, target: {self.table_rows:,})")
            else:
                print(f"   📝 Table does not exist - will be created")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Could not check table info: {e}")
        
        print("")  # Add spacing
    
    def ensure_indexes(self):
        """Create/verify indexes for point lookup queries (smart - only drops existing)"""
        print(f"🏗️  Setting up point lookup index on {self.test_table}...")
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check which indexes actually exist before dropping
            cursor.execute(f"""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = '{self.test_table}' 
                AND indexname LIKE 'idx_{self.test_table}_%'
                AND indexname != 'idx_{self.test_table}_user_id';
            """)
            existing_indexes = [row[0] for row in cursor.fetchall()]
            
            if existing_indexes:
                print(f"   🗑️  Dropping {len(existing_indexes)} existing indexes...")
                for index_name in existing_indexes:
                    cursor.execute(f"DROP INDEX {index_name};")
                    print(f"   ❌ Dropped {index_name}")
            else:
                print(f"   ✅ No unnecessary indexes to drop")
            
            # Create user_id index for point lookup queries
            index_name = f"idx_{self.test_table}_user_id"
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.test_table}(user_id);")
            print(f"   ✅ Ensured index on user_id exists")
            
            # Analyze table for query planner statistics
            cursor.execute(f"ANALYZE {self.test_table};")
            print(f"   📊 Updated table statistics")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✅ Point lookup index ready for testing")
            return True
            
        except Exception as e:
            print(f"❌ Index setup failed: {e}")
            return False
    
    def setup_table(self):
        """Create and populate test table (smart - checks existing)"""
        print(f"🛠️  Setting up {self.table_rows:,} row test table...")
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if table exists and has correct row count
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{self.test_table}';
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                cursor.execute(f"SELECT COUNT(*) FROM {self.test_table};")
                existing_count = cursor.fetchone()[0]
                
                if existing_count == self.table_rows:
                    print(f"✅ Table already exists with {existing_count:,} rows - skipping creation")
                    cursor.close()
                    conn.close()
                    return True
                else:
                    print(f"⚠️  Table has {existing_count:,} rows (need {self.table_rows:,}) - recreating...")
            else:
                print(f"📋 Creating new table...")
            
            # Drop and create table
            cursor.execute(f"DROP TABLE IF EXISTS {self.test_table};")
            cursor.execute(f"""
                CREATE TABLE {self.test_table} (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    product_id INTEGER,
                    category VARCHAR(50),
                    amount DECIMAL(10,2),
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            print(f"📊 Inserting {self.table_rows:,} test records...")
            
            for batch in range(0, self.table_rows, self.batch_size):
                values = []
                for i in range(batch, min(batch + self.batch_size, self.table_rows)):
                    values.append(f"({i % 10000}, {i % 1000}, 'category_{i % 20}', "
                                f"{(i * 1.5) % 1000:.2f}, 'active', NOW(), NOW())")
                
                cursor.execute(f"""
                    INSERT INTO {self.test_table} 
                    (user_id, product_id, category, amount, status, created_at, updated_at) 
                    VALUES {','.join(values)}
                """)
                
                if (batch + self.batch_size) % 100000 == 0:
                    print(f"   Inserted {batch + self.batch_size:,} records...")
            
            conn.commit()
            cursor.execute(f"SELECT COUNT(*) FROM {self.test_table};")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            print(f"✅ Table ready with {count:,} records and indexes")
            return True
            
        except Exception as e:
            print(f"❌ Table setup failed: {e}")
            return False
    
    def run_query_worker(self, worker_id, stop_event, results_queue):
        """Worker thread that runs queries until stopped"""
        
        # Point lookup queries using only user_id index
        queries = [
            (f"SELECT * FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT * FROM {self.test_table} WHERE user_id = %s LIMIT 1;", 'int'),
            (f"SELECT user_id, amount, status FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT COUNT(*) FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT user_id, category, amount FROM {self.test_table} WHERE user_id = %s LIMIT 5;", 'int')
        ]
        
        query_count = 0
        error_count = 0
        connection_error_count = 0
        latencies = []
        
        conn = None
        cursor = None
        
        while not stop_event.is_set():
            try:
                # Establish connection if needed
                if conn is None:
                    conn = self.get_connection(quiet=True)
                    cursor = conn.cursor()
                
                query, param_type = random.choice(queries)
                
                # Generate random user_id parameter (all queries use int)
                param = random.randint(1, 10000)
                
                start_time = time.time()
                cursor.execute(query, (param,))
                cursor.fetchall()  # Consume results
                end_time = time.time()
                
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                query_count += 1
                
            except Exception as e:
                error_count += 1
                
                # Check if it's a connection-related error
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ['connection', 'password authentication', 'server', 'fatal']):
                    connection_error_count += 1
                    # Reset connection on connection errors
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None
                        cursor = None
                
        # Clean up connection
        if conn:
            try:
                cursor.close()
                conn.close()
            except:
                pass
        
        results_queue.append({
            'worker_id': worker_id,
            'query_count': query_count,
            'error_count': error_count,
            'connection_error_count': connection_error_count,
            'latencies': latencies
        })
    
    def run_pooled_query_worker(self, worker_id, stop_event, results_queue, connection_pool):
        """Worker thread that uses connection pooling for multi-query benchmark"""
        queries = [
            (f"SELECT * FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT * FROM {self.test_table} WHERE user_id = %s LIMIT 1;", 'int'),
            (f"SELECT user_id, amount, status FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT COUNT(*) FROM {self.test_table} WHERE user_id = %s;", 'int'),
            (f"SELECT user_id, category, amount FROM {self.test_table} WHERE user_id = %s LIMIT 5;", 'int')
        ]
        
        query_count = 0
        error_count = 0
        connection_error_count = 0
        latencies = []
        
        conn = None
        cursor = None
        
        while not stop_event.is_set():
            try:
                # Get connection from pool if needed
                if conn is None:
                    conn = connection_pool.get_connection(timeout=10)
                    cursor = conn.cursor()
                
                query, param_type = random.choice(queries)
                
                # Generate random user_id parameter (all queries use int)
                param = random.randint(1, 10000)
                
                start_time = time.time()
                cursor.execute(query, (param,))
                cursor.fetchall()  # Consume results
                end_time = time.time()
                
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                query_count += 1
                
            except Exception as e:
                error_count += 1
                
                # Check if it's a connection-related error
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ['connection', 'password authentication', 'server', 'fatal']):
                    connection_error_count += 1
                    # Return broken connection and get a new one
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None
                        cursor = None
                
        # Return connection to pool
        if conn:
            connection_pool.return_connection(conn)
        
        results_queue.append({
            'worker_id': worker_id,
            'query_count': query_count,
            'error_count': error_count,
            'connection_error_count': connection_error_count,
            'latencies': latencies
        })
    
    def run_concurrent_test(self, thread_count):
        """Run concurrent test with specified thread count"""
        print(f"\n🚀 Testing {thread_count} concurrent threads...")
        
        # Setup connection pool if enabled
        if self.use_connection_pooling:
            pool_key = f"pool_{thread_count}"
            if pool_key not in self.connection_pools:
                initial_pool_size = max(thread_count, 20)
                print(f"📋 Creating connection pool: {thread_count} threads → {initial_pool_size} connections")
                self.connection_pools[pool_key] = EnhancedProductionConnectionPool(
                    auth_manager=self.auth_manager
                )
            
            connection_pool = self.connection_pools[pool_key]
            connection_pool.initialize_pool()
            print(f"🏃 Starting {self.test_duration}s multi-query benchmark (pool ready)...")
        else:
            connection_pool = None
            print(f"🏃 Starting {self.test_duration}s multi-query benchmark (individual connections)...")
        
        stop_event = threading.Event()
        results_queue = []
        threads = []
        
        # Start worker threads
        for i in range(thread_count):
            if self.use_connection_pooling:
                thread = threading.Thread(
                    target=self.run_pooled_query_worker,
                    args=(i, stop_event, results_queue, connection_pool)
                )
            else:
                thread = threading.Thread(
                    target=self.run_query_worker,
                    args=(i, stop_event, results_queue)
                )
            thread.start()
            threads.append(thread)
        
        # Let it run for the test duration
        time.sleep(self.test_duration)
        
        # Stop all threads
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5)
        
        # Collect results
        total_queries = sum(r['query_count'] for r in results_queue)
        total_errors = sum(r['error_count'] for r in results_queue)
        total_connection_errors = sum(r.get('connection_error_count', 0) for r in results_queue)
        all_latencies = []
        for r in results_queue:
            all_latencies.extend(r['latencies'])
        
        # Calculate total operations attempted (successful + failed)
        total_operations_attempted = total_queries + total_errors
        
        if total_operations_attempted > 0:
            throughput = total_queries / self.test_duration
            error_rate = (total_errors / total_operations_attempted) * 100
            
            # Calculate latency stats only if we have successful operations
            if all_latencies:
                avg_latency = statistics.mean(all_latencies)
                p95_latency = statistics.quantiles(all_latencies, n=20)[18]  # 95th percentile
                p99_latency = statistics.quantiles(all_latencies, n=100)[98]  # 99th percentile
            else:
                avg_latency = p95_latency = p99_latency = 0
            
            result = {
                'threads': thread_count,
                'throughput_ops_sec': throughput,
                'total_operations': total_queries,
                'total_operations_attempted': total_operations_attempted,
                'avg_latency_ms': avg_latency,
                'p95_latency_ms': p95_latency,
                'p99_latency_ms': p99_latency,
                'error_count': total_errors,
                'connection_error_count': total_connection_errors,
                'error_rate_pct': error_rate
            }
            
            print(f"   Throughput: {throughput:.1f} queries/sec")
            if all_latencies:
                print(f"   Latency: {avg_latency:.1f}ms avg, {p95_latency:.1f}ms p95, {p99_latency:.1f}ms p99")
            else:
                print(f"   Latency: N/A (no successful operations)")
            print(f"   Errors: {total_errors} total ({error_rate:.2f}%) - {total_connection_errors} connection failures")
            
            return result
        else:
            print(f"❌ No operations attempted")
            return None
    
    def run_benchmark(self):
        """Run complete benchmark suite"""
        print(f"🎯 Starting Lakebase {self.table_rows:,} Row High-Concurrency Benchmark")
        print("=" * 70)
        
        # Test connection
        if not self.auth_manager.test_connection():
            print("❌ Authentication failed")
            return False
        
        # Display current table status
        self.display_table_info()
        
        # Setup table
        if not self.setup_table():
            print("❌ Table setup failed")
            return False
        
        # Ensure indexes are ready for high-concurrency testing
        if not self.ensure_indexes():
            print("❌ Index setup failed")
            return False
        
        # Run concurrency tests
        print(f"\n🏃 Running high-concurrency tests...")
        for thread_count in self.thread_counts:
            result = self.run_concurrent_test(thread_count)
            if result:
                self.results.append(result)
        
        # Generate summary
        self.generate_summary()
        
        # Cleanup connection pools
        if self.use_connection_pooling:
            print("🧹 Cleaning up connection pools...")
            for pool_key, pool in self.connection_pools.items():
                pool.close_all()
            self.connection_pools.clear()
        
        return True
    
    def generate_summary(self):
        """Generate performance summary"""
        if not self.results:
            print("No results to summarize")
            return
        
        print("\n" + "=" * 70)
        print(f"🏆 LAKEBASE {self.table_rows:,} ROW BENCHMARK SUMMARY")
        print("=" * 70)
        print(f"{'Threads':<8} {'Queries/sec':<12} {'Avg Lat':<10} {'P95 Lat':<10} {'P99 Lat':<10} {'Errors':<15} {'Conn Err':<8}")
        print("-" * 80)
        
        best_throughput = 0
        best_config = None
        
        for result in self.results:
            conn_errs = result.get('connection_error_count', 0)
            error_display = f"{result['error_count']} ({result['error_rate_pct']:.1f}%)"
            
            print(f"{result['threads']:<8} "
                  f"{result['throughput_ops_sec']:<12.1f} "
                  f"{result['avg_latency_ms']:<10.1f} "
                  f"{result['p95_latency_ms']:<10.1f} "
                  f"{result['p99_latency_ms']:<10.1f} "
                  f"{error_display:<15} "
                  f"{conn_errs:<8}")
            
            # Find best performance considering error rate (prefer low error rates)
            if result['error_rate_pct'] == 0 and result['throughput_ops_sec'] > best_throughput:
                best_throughput = result['throughput_ops_sec']
                best_config = result
            elif best_config is None or (best_config['error_rate_pct'] > 0 and result['error_rate_pct'] < best_config['error_rate_pct']):
                best_config = result
        
        if best_config:
            print(f"\n🏆 BEST PERFORMANCE:")
            print(f"   Threads: {best_config['threads']} concurrent connections")
            print(f"   Peak Throughput: {best_config['throughput_ops_sec']:.1f} queries/sec")
            print(f"   Latency: {best_config['avg_latency_ms']:.1f}ms avg, {best_config['p99_latency_ms']:.1f}ms p99")
            print(f"   Error Rate: {best_config['error_rate_pct']:.2f}%")
            conn_errs = best_config.get('connection_error_count', 0)
            if conn_errs > 0:
                print(f"   Connection Errors: {conn_errs}")
            
        # Show warning if high error rates detected
        high_error_configs = [r for r in self.results if r['error_rate_pct'] > 10]
        if high_error_configs:
            print(f"\n⚠️  WARNING: High error rates detected at {len(high_error_configs)} thread counts")
            print(f"   Consider using lower concurrency for reliable performance")
        
        # Create timestamped results folder
        import csv
        import os
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        from secure_config_finder import get_results_directory
        results_base = get_results_directory()
        results_dir = f"{results_base}/{timestamp}"
        os.makedirs(results_dir, exist_ok=True)
        
        # Save CSV results
        csv_filename = f"lakebase_{self.table_rows//1000}k_benchmark_{timestamp}.csv"
        csv_path = os.path.join(results_dir, csv_filename)
        with open(csv_path, 'w', newline='') as f:
            if self.results:
                writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
                writer.writeheader()
                writer.writerows(self.results)
        
        # Generate summary report
        report_filename = f"benchmark_report_{timestamp}.md"
        report_path = os.path.join(results_dir, report_filename)
        self._generate_report(report_path, timestamp)
        
        print(f"\n📊 Results saved to folder: {results_dir}/")
        print(f"   📈 CSV data: {csv_filename}")
        print(f"   📄 Report: {report_filename}")
    
    def _generate_report(self, report_path, timestamp):
        """Generate a detailed markdown report"""
        with open(report_path, 'w') as f:
            f.write(f"# Lakebase Benchmark Report\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Configuration section
            f.write(f"## 🔧 Configuration\n")
            f.write(f"- **Table:** {self.test_table}\n")
            f.write(f"- **Rows:** {self.table_rows:,}\n")
            f.write(f"- **Thread Counts:** {self.thread_counts}\n")
            f.write(f"- **Test Duration:** {self.test_duration}s per thread count\n")
            f.write(f"- **Batch Size:** {self.batch_size:,}\n\n")
            
            # Explanation section
            f.write(f"## 📖 Understanding the Metrics\n\n")
            f.write(f"### 🧵 What are Threads?\n")
            f.write(f"Each **thread** represents a **concurrent database connection** simulating a real user or application:\n")
            f.write(f"- **1 thread** = 1 simultaneous user querying the database\n")
            f.write(f"- **32 threads** = 32 users hitting the database at the same time\n")
            f.write(f"- **96 threads** = 96 concurrent applications making requests\n\n")
            f.write(f"This tests how well Lakebase handles **concurrent workloads** and identifies **OAuth token limits**.\n\n")
            
            f.write(f"### ⚡ What is Throughput?\n")
            f.write(f"**Throughput** measures **successful queries per second** (ops/sec):\n")
            f.write(f"- Higher throughput = better database performance\n")
            f.write(f"- Only **successful** queries are counted (failed queries are excluded)\n")
            f.write(f"- Example: 470.2 ops/sec means the database successfully processed 470 queries every second\n\n")
            f.write(f"Each query is a point lookup like: `SELECT * FROM {self.test_table} WHERE user_id = 1234`\n\n")
            
            # Performance summary
            f.write(f"## 📊 Performance Summary\n")
            f.write(f"| Threads | Throughput (queries/sec) | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Error Rate |\n")
            f.write(f"|---------|--------------------------|------------------|------------------|------------------|------------|\n")
            
            for result in self.results:
                f.write(f"| {result['threads']} | {result['throughput_ops_sec']:.1f} | "
                       f"{result['avg_latency_ms']:.1f} | {result['p95_latency_ms']:.1f} | "
                       f"{result['p99_latency_ms']:.1f} | {result['error_rate_pct']:.2f}% |\n")
            
            # Best performance
            if self.results:
                best_config = max(self.results, key=lambda x: x['throughput_ops_sec'])
                f.write(f"\n## 🏆 Best Performance\n")
                f.write(f"- **Threads:** {best_config['threads']} concurrent connections\n")
                f.write(f"- **Peak Throughput:** {best_config['throughput_ops_sec']:.1f} queries/sec\n")
                f.write(f"- **Average Latency:** {best_config['avg_latency_ms']:.1f}ms\n")
                f.write(f"- **P99 Latency:** {best_config['p99_latency_ms']:.1f}ms\n")
                f.write(f"- **Error Rate:** {best_config['error_rate_pct']:.2f}%\n")
            
            # Query types
            f.write(f"\n## 🔍 Point Lookup Query Types Tested\n")
            f.write(f"1. **Full Record:** `SELECT * FROM {self.test_table} WHERE user_id = ?`\n")
            f.write(f"2. **Single Record:** `SELECT * FROM {self.test_table} WHERE user_id = ? LIMIT 1`\n")
            f.write(f"3. **Specific Columns:** `SELECT user_id, amount, status FROM {self.test_table} WHERE user_id = ?`\n")
            f.write(f"4. **Count Query:** `SELECT COUNT(*) FROM {self.test_table} WHERE user_id = ?`\n")
            f.write(f"5. **Limited Columns:** `SELECT user_id, category, amount FROM {self.test_table} WHERE user_id = ? LIMIT 5`\n")
            
            # Indexes
            f.write(f"\n## 🏗️ Index Created\n")
            f.write(f"- `idx_{self.test_table}_user_id` on user_id *(only index for pure point lookup testing)*\n")
            
            f.write(f"\n---\n")
            f.write(f"*Report generated by Lakebase Benchmark Tool*\n")

def main():
    """Main function"""
    benchmark = LakebaseBenchmark()
    success = benchmark.run_benchmark()
    
    if success:
        print("\n✅ Benchmark completed successfully!")
    else:
        print("\n❌ Benchmark failed!")

if __name__ == "__main__":
    main()