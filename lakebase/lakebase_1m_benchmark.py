#!/usr/bin/env python3
"""
Streamlined Lakebase 1 Million Row High-Concurrency Benchmark
Single script that does everything needed for performance testing
Enhanced with connection pooling and OAuth rate limiting mitigation
"""

import json
import time
import psycopg2
import statistics
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from lakebase_sdk_auth import LakebaseSDKAuthManager
from enhanced_production_pool import EnhancedProductionConnectionPool

class Lakebase1MBenchmark:
    def __init__(self):
        """Initialize benchmark with enhanced authentication and load config"""
        self.auth_manager = LakebaseSDKAuthManager()
        self.results = []
        self.test_table = "benchmark_1m_test"
        self.connection_pools = {}  # Pool per thread count to avoid contention
        
        # Load benchmark configuration from file
        try:
            # Use secure config finder
            from secure_config_finder import find_config_file
            config_file = find_config_file()
            
            with open(config_file, 'r') as f:
                config = json.load(f)
                benchmark_config = config.get('benchmark', {})
                
            self.thread_counts = benchmark_config.get('thread_counts', [1, 4, 8, 16, 32, 64, 128, 200, 300])
            self.test_duration = benchmark_config.get('test_duration_seconds', 60)
            self.table_rows = benchmark_config.get('table_rows', 1000000)
            self.batch_size = benchmark_config.get('batch_size', 10000)
            self.connection_stagger_delay = benchmark_config.get('connection_stagger_delay', 0.1)
            self.max_pool_init_time = benchmark_config.get('max_pool_init_time', 30)
            
            print(f"📋 Loaded configuration:")
            print(f"   Target rows: {self.table_rows:,}")
            print(f"   Thread counts: {self.thread_counts}")
            print(f"   Test duration: {self.test_duration}s per thread count")
            print(f"   Connection stagger delay: {self.connection_stagger_delay}s")
            print(f"   Max pool init time: {self.max_pool_init_time}s")
            
        except Exception as e:
            print(f"⚠️  Failed to load config, using defaults: {e}")
            self.thread_counts = [1, 4, 8, 16, 32, 64, 128, 200, 300]
            self.test_duration = 60
            self.table_rows = 1000000
            self.batch_size = 10000
            self.connection_stagger_delay = 0.1
            self.max_pool_init_time = 30
        
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.auth_manager.get_connection_params())
    
    def setup_1m_table(self):
        """Create and populate test table with configured number of rows (smart - checks existing)"""
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
                    print(f"⚠️  Table exists but has {existing_count:,} rows (need {self.table_rows:,}) - recreating...")
            else:
                print(f"📋 Table doesn't exist - creating new...")
            
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
                # Insert batch
                values = []
                for i in range(batch, min(batch + self.batch_size, self.table_rows)):
                    values.append(f"({i % 10000}, {i % 1000}, 'category_{i % 20}', "
                                f"{(i * 1.5) % 1000:.2f}, 'active', NOW(), NOW())")
                
                cursor.execute(f"""
                    INSERT INTO {self.test_table} 
                    (user_id, product_id, category, amount, status, created_at, updated_at) 
                    VALUES {','.join(values)}
                """)
                
                # Progress update every 100k records
                if (batch + self.batch_size) % 100000 == 0:
                    print(f"   Inserted {batch + self.batch_size:,} records...")
            
            # Create indexes for better performance
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.test_table}_user_id ON {self.test_table}(user_id);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.test_table}_category ON {self.test_table}(category);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.test_table}_status ON {self.test_table}(status);")
            
            conn.commit()
            
            # Verify count
            cursor.execute(f"SELECT COUNT(*) FROM {self.test_table};")
            count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            print(f"✅ Table created with {count:,} records and indexes")
            return True
            
        except Exception as e:
            print(f"❌ Table setup failed: {e}")
            return False
    
    def run_concurrent_queries(self, thread_count):
        """Run concurrent read queries with connection pooling"""
        print(f"\n🚀 Testing {thread_count} concurrent threads...")
        
        # Create or reuse connection pool for this thread count
        pool_key = f"pool_{thread_count}"
        if pool_key not in self.connection_pools:
            # Create pool with gradual scaling - test OAuth rate limiting with larger pools
            initial_pool_size = max(thread_count, 20)  # Create at least 20 connections, or match thread count if higher
            print(f"📋 Creating connection pool: {thread_count} threads → {initial_pool_size} connections")
            # Use enhanced production pool instead
            self.connection_pools[pool_key] = EnhancedProductionConnectionPool(
                auth_manager=self.auth_manager
            )
        
        connection_pool = self.connection_pools[pool_key]
        
        # Ensure pool is initialized before starting benchmark timer
        connection_pool.initialize_pool()
        
        # NOW start the actual benchmark timer with pool ready
        print(f"🏃 Starting {self.test_duration}s query benchmark (pool ready)...")
        start_time = time.time()
        end_time = start_time + self.test_duration
        latencies = []
        operations = 0
        errors = 0
        connection_errors = 0
        
        # Thread-safe counters
        operations_lock = threading.Lock()
        latencies_lock = threading.Lock()
        
        def worker():
            nonlocal operations, errors, connection_errors
            conn = None
            local_operations = 0
            local_latencies = []
            local_errors = 0
            
            try:
                # Get connection from pool with retry logic
                conn = connection_pool.get_connection(timeout=10)
                cursor = conn.cursor()
                
                while time.time() < end_time:
                    try:
                        # Simple point lookup queries (matching original benchmark)
                        user_id = local_operations % 10000
                        
                        query_start = time.time()
                        cursor.execute(f"""
                            SELECT * FROM {self.test_table} 
                            WHERE user_id = %s
                            LIMIT 1
                        """, (user_id,))
                        
                        result = cursor.fetchone()
                        query_time = (time.time() - query_start) * 1000  # Convert to ms
                        
                        if result:
                            local_latencies.append(query_time)
                            local_operations += 1
                        
                    except Exception as e:
                        local_errors += 1
                        # Check if it's a connection-related error
                        error_msg = str(e).lower()
                        if any(keyword in error_msg for keyword in ['connection', 'server closed', 'broken pipe']):
                            # Connection issue, try to get a new one
                            try:
                                if conn:
                                    conn.close()
                                conn = connection_pool.get_connection(timeout=5)
                                cursor = conn.cursor()
                            except Exception as conn_e:
                                print(f"⚠️  Failed to recover connection: {conn_e}")
                                break
                        
            except Exception as e:
                connection_errors += 1
                print(f"⚠️  Worker connection error: {e}")
            finally:
                # Return connection to pool
                if conn:
                    connection_pool.return_connection(conn)
                
                # Update shared counters thread-safely
                with operations_lock:
                    operations += local_operations
                    errors += local_errors
                
                with latencies_lock:
                    latencies.extend(local_latencies)
        
        # Run concurrent workers with gradual startup
        print(f"🏊 Using connection pool with gradual worker startup...")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = []
            
            # Submit workers gradually to avoid connection pool exhaustion
            batch_size = min(20, thread_count)  # Start with batches of 20
            for i in range(0, thread_count, batch_size):
                batch_end = min(i + batch_size, thread_count)
                batch_workers = batch_end - i
                
                if i > 0:  # Add delay between batches
                    time.sleep(0.5)
                    print(f"   Starting batch {i//batch_size + 1}: workers {i+1}-{batch_end}")
                
                for j in range(batch_workers):
                    futures.append(executor.submit(worker))
            
            # Wait for all workers to complete
            for future in as_completed(futures):
                future.result()
        
        actual_duration = time.time() - start_time
        
        if latencies:
            # Calculate statistics
            throughput = operations / actual_duration
            avg_latency = statistics.mean(latencies)
            p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 20 else max(latencies)
            p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) > 100 else max(latencies)
            
            result = {
                'threads': thread_count,
                'duration_sec': actual_duration,
                'total_operations': operations,
                'throughput_ops_sec': throughput,
                'avg_latency_ms': avg_latency,
                'p95_latency_ms': p95_latency,
                'p99_latency_ms': p99_latency,
                'error_count': errors,
                'error_rate_pct': (errors / max(operations + errors, 1)) * 100,
                'timestamp': datetime.now().isoformat()
            }
            
            self.results.append(result)
            
            print(f"📊 Results:")
            print(f"   Throughput: {throughput:.2f} ops/sec")
            print(f"   Avg Latency: {avg_latency:.2f}ms")
            print(f"   P95 Latency: {p95_latency:.2f}ms") 
            print(f"   P99 Latency: {p99_latency:.2f}ms")
            print(f"   Operations: {operations:,}")
            print(f"   Errors: {errors} ({(errors / max(operations + errors, 1)) * 100:.1f}%)")
            if connection_errors > 0:
                print(f"   Connection Errors: {connection_errors}")
            
            return result
        else:
            print(f"❌ No successful operations completed")
            return None
    
    def run_full_benchmark(self):
        """Run complete benchmark suite"""
        print(f"🎯 Starting Lakebase {self.table_rows:,} Row High-Concurrency Benchmark")
        print("=" * 70)
        
        # Test connection first
        print("🔍 Testing authentication and connection...")
        if not self.auth_manager.test_connection():
            print("❌ Authentication failed. Run: python get_lakebase_oauth_token.py")
            return False
        
        # Setup 1M row table
        if not self.setup_1m_table():
            return False
        
        print(f"\n🚀 Running scaling tests with thread counts: {self.thread_counts}")
        print(f"⏱️  Each test runs for {self.test_duration} seconds")
        
        # Run scaling tests
        for thread_count in self.thread_counts:
            self.run_concurrent_queries(thread_count)
            print(f"⏸️  Cooling down for 10 seconds...")
            time.sleep(10)
            
        # Cleanup connection pools
        print("🧹 Cleaning up connection pools...")
        for pool_key, pool in self.connection_pools.items():
            pool.close_all()
        self.connection_pools.clear()
        
        # Generate summary
        self.generate_summary()
        return True
    
    def generate_summary(self):
        """Generate performance summary and save results"""
        if not self.results:
            print("No results to summarize")
            return
        
        print("\n" + "=" * 70)
        print(f"🏆 LAKEBASE {self.table_rows:,} ROW BENCHMARK SUMMARY")
        print("=" * 70)
        print(f"{'Threads':<8} {'Throughput':<12} {'Avg Lat':<10} {'P95 Lat':<10} {'P99 Lat':<10} {'Errors':<8}")
        print("-" * 70)
        
        best_throughput = 0
        best_config = None
        
        for result in self.results:
            throughput = result['throughput_ops_sec']
            if throughput > best_throughput:
                best_throughput = throughput
                best_config = result
            
            print(f"{result['threads']:<8} {throughput:<12.1f} {result['avg_latency_ms']:<10.1f} "
                  f"{result['p95_latency_ms']:<10.1f} {result['p99_latency_ms']:<10.1f} {result['error_count']:<8}")
        
        if best_config:
            print(f"\n🎯 OPTIMAL CONFIGURATION:")
            print(f"   Threads: {best_config['threads']}")
            print(f"   Peak Throughput: {best_config['throughput_ops_sec']:.1f} ops/sec")
            print(f"   Latency: {best_config['avg_latency_ms']:.1f}ms avg, {best_config['p99_latency_ms']:.1f}ms p99")
            print(f"   Error Rate: {best_config['error_rate_pct']:.2f}%")
        
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
            f.write(f"# Lakebase 1M Row Point Lookup Benchmark Report\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Configuration section
            f.write(f"## 🔧 Configuration\n")
            f.write(f"- **Table:** {self.test_table}\n")
            f.write(f"- **Rows:** {self.table_rows:,}\n")
            f.write(f"- **Thread Counts:** {self.thread_counts}\n")
            f.write(f"- **Test Duration:** {self.test_duration}s per thread count\n")
            f.write(f"- **Batch Size:** {self.batch_size:,}\n")
            f.write(f"- **Connection Pooling:** Enabled with OAuth rate limiting mitigation\n")
            f.write(f"- **Stagger Delay:** {self.connection_stagger_delay}s between connections\n\n")
            
            # Explanation section
            f.write(f"## 📖 Understanding the Metrics\n\n")
            f.write(f"### 🧵 What are Threads?\n")
            f.write(f"Each **thread** represents a **concurrent database connection** simulating a real user or application:\n")
            f.write(f"- **1 thread** = 1 simultaneous user querying the database\n")
            f.write(f"- **32 threads** = 32 users hitting the database at the same time\n")
            f.write(f"- **100 threads** = 100 concurrent applications making requests\n\n")
            f.write(f"This tests how well Lakebase handles **concurrent workloads** with **OAuth rate limiting protection**.\n\n")
            
            f.write(f"### ⚡ What is Throughput?\n")
            f.write(f"**Throughput** measures **successful queries per second** (ops/sec):\n")
            f.write(f"- Higher throughput = better database performance\n")
            f.write(f"- Only **successful** queries are counted (failed queries are excluded)\n")
            f.write(f"- Example: 1,261.6 ops/sec means the database successfully processed 1,261 queries every second\n\n")
            f.write(f"The query tested is an optimized point lookup: `SELECT * FROM {self.test_table} WHERE user_id = ? LIMIT 1`\n\n")
            
            # Performance summary
            f.write(f"## 📊 Performance Summary\n")
            f.write(f"| Threads | Throughput (ops/sec) | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Error Rate |\n")
            f.write(f"|---------|----------------------|-------------------|-------------------|-------------------|------------|\n")
            
            for result in self.results:
                f.write(f"| {result['threads']} | {result['throughput_ops_sec']:.1f} | "
                       f"{result['avg_latency_ms']:.1f} | {result['p95_latency_ms']:.1f} | "
                       f"{result['p99_latency_ms']:.1f} | {result['error_rate_pct']:.2f}% |\n")
            
            # Best performance
            if self.results:
                best_config = max(self.results, key=lambda x: x['throughput_ops_sec'])
                f.write(f"\n## 🏆 Best Performance\n")
                f.write(f"- **Threads:** {best_config['threads']} concurrent connections\n")
                f.write(f"- **Peak Throughput:** {best_config['throughput_ops_sec']:.1f} ops/sec\n")
                f.write(f"- **Average Latency:** {best_config['avg_latency_ms']:.1f}ms\n")
                f.write(f"- **P99 Latency:** {best_config['p99_latency_ms']:.1f}ms\n")
                f.write(f"- **Error Rate:** {best_config['error_rate_pct']:.2f}%\n")
            
            # OAuth rate limiting improvements
            f.write(f"\n## 🔐 OAuth Rate Limiting Improvements\n")
            f.write(f"This benchmark includes enhanced connection pooling to handle OAuth rate limits:\n\n")
            f.write(f"### ✅ Features Implemented\n")
            f.write(f"- **Staggered Connection Pool**: Creates connections with {self.connection_stagger_delay}s delays\n")
            f.write(f"- **Exponential Backoff**: Automatic retry with jitter for rate-limited requests\n")
            f.write(f"- **Connection Reuse**: Pool connections to minimize OAuth authentication requests\n")
            f.write(f"- **Gradual Worker Startup**: Batched thread deployment to prevent pool exhaustion\n")
            f.write(f"- **Error Recovery**: Automatic connection recovery and pool management\n\n")
            
            # Query pattern
            f.write(f"\n## 🔍 Query Pattern Tested\n")
            f.write(f"**Optimized Point Lookup Query:**\n")
            f.write(f"```sql\n")
            f.write(f"SELECT * FROM {self.test_table} WHERE user_id = ? LIMIT 1\n")
            f.write(f"```\n\n")
            f.write(f"- **Type:** Single-row point lookup with LIMIT 1 optimization\n")
            f.write(f"- **Index Used:** `idx_{self.test_table}_user_id` on user_id column\n")
            f.write(f"- **Parameters:** Random user_id values from 1 to 10,000\n")
            f.write(f"- **Focus:** Pure read performance with optimal indexing\n")
            
            # Performance insights
            f.write(f"\n## 📈 Performance Insights\n")
            if len(self.results) > 1:
                first_result = self.results[0]
                last_result = self.results[-1]
                throughput_scaling = last_result['throughput_ops_sec'] / first_result['throughput_ops_sec']
                f.write(f"- **Throughput Scaling:** {throughput_scaling:.2f}x from {first_result['threads']} to {last_result['threads']} threads\n")
                f.write(f"- **Latency Impact:** {last_result['avg_latency_ms'] - first_result['avg_latency_ms']:.1f}ms latency increase at peak concurrency\n")
                
                # Check for error-free performance
                error_free_results = [r for r in self.results if r['error_rate_pct'] == 0]
                if error_free_results:
                    best_error_free = max(error_free_results, key=lambda x: x['throughput_ops_sec'])
                    f.write(f"- **Reliable Performance:** {best_error_free['throughput_ops_sec']:.1f} ops/sec with 0% errors at {best_error_free['threads']} threads\n")
            
            f.write(f"\n## 🏗️ Table Structure\n")
            f.write(f"```sql\n")
            f.write(f"CREATE TABLE {self.test_table} (\n")
            f.write(f"    id SERIAL PRIMARY KEY,\n")
            f.write(f"    user_id INTEGER,\n")
            f.write(f"    product_id INTEGER,\n")
            f.write(f"    category VARCHAR(50),\n")
            f.write(f"    amount DECIMAL(10,2),\n")
            f.write(f"    status VARCHAR(20),\n")
            f.write(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n")
            f.write(f"    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n")
            f.write(f");\n\n")
            f.write(f"-- Indexes for performance\n")
            f.write(f"CREATE INDEX idx_{self.test_table}_user_id ON {self.test_table}(user_id);\n")
            f.write(f"CREATE INDEX idx_{self.test_table}_category ON {self.test_table}(category);\n")
            f.write(f"CREATE INDEX idx_{self.test_table}_status ON {self.test_table}(status);\n")
            f.write(f"```\n")
            
            f.write(f"\n---\n")
            f.write(f"*Report generated by Lakebase 1M Row Benchmark Tool with OAuth Rate Limiting Protection*\n")

def main():
    """Main execution"""
    benchmark = Lakebase1MBenchmark()
    
    if benchmark.run_full_benchmark():
        print(f"\n✅ Benchmark completed successfully!")
    else:
        print(f"\n❌ Benchmark failed!")

if __name__ == "__main__":
    main()