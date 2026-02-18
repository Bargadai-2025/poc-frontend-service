import asyncio
from typing import List, Dict, Any, Tuple
from app.scoreplex_client import ScoreplexClient
from app.excel_handler import ExcelHandler
from app.config import settings

class BulkProcessor:
    def __init__(self, batch_size: int = None):
        self.client = ScoreplexClient()
        self.excel_handler = ExcelHandler()
        self.batch_size = batch_size if batch_size is not None else settings.BATCH_SIZE

    async def process_bulk(self, input_content: bytes) -> Tuple[Dict[str, Any], bytes]:
        """
        Process Excel from bytes (in-memory). Returns (summary, result_excel_bytes).
        No files are stored on disk.
        """
        try:
            # Step 1: Read input from bytes
            rows = self.excel_handler.read_input_excel_from_bytes(input_content)
            total_rows = len(rows)
            
            if total_rows == 0:
                raise ValueError("No data rows found in Excel")
            
            print(f"\n🚀 Processing {total_rows} rows...")
            print(f"📊 Batch size: {self.batch_size}")
            
            total_batches = (total_rows + self.batch_size - 1) // self.batch_size
            print(f"📊 Total batches: {total_batches}")
            print(f"⏱️ Estimated time: {self._estimate_time(total_rows)}\n")
            
            # Step 2: Process in batches
            all_results = []
            
            for batch_num in range(0, total_rows, self.batch_size):
                batch = rows[batch_num:batch_num + self.batch_size]
                current_batch = (batch_num // self.batch_size) + 1
                
                print(f"🔄 Batch {current_batch}/{total_batches} ({len(batch)} rows) - Processing...")
                
                # Process entire batch concurrently
                tasks = [
                    self.client.process_row(
                        email=row.get("email", ""),
                        phone=row.get("phone", ""),
                        ip=row.get("ip") or None
                    )
                    for row in batch
                ]
                
                batch_results = await asyncio.gather(*tasks)
                all_results.extend(batch_results)
                
                # Show batch summary
                success = sum(1 for r in batch_results if r.get("status") == "SUCCESS")
                incomplete = sum(1 for r in batch_results if r.get("status") == "INCOMPLETE")
                failed = sum(1 for r in batch_results if r.get("status") == "FAILED")
                print(f"✅ Batch {current_batch} complete: {success} success, {incomplete} incomplete, {failed} failed\n")
            
            # Step 3: Build output Excel in memory (no file saved)
            print("📝 Building result Excel...")
            output_bytes = self.excel_handler.write_output_excel_to_bytes(all_results)
            
            # Step 4: Summary
            success_count = sum(1 for r in all_results if r.get("status") == "SUCCESS")
            incomplete_count = sum(1 for r in all_results if r.get("status") == "INCOMPLETE")
            failed_count = sum(1 for r in all_results if r.get("status") == "FAILED")
            
            summary = {
                "total_rows": total_rows,
                "success": success_count,
                "incomplete": incomplete_count,
                "failed": failed_count,
            }
            
            print(f"\n✅ Processing complete!")
            print(f"   Total rows: {total_rows}")
            print(f"   Successful (all 3 checks complete): {success_count}")
            print(f"   Incomplete (timeout or pending): {incomplete_count}")
            print(f"   Failed: {failed_count}")
            
            return summary, output_bytes
            
        except Exception as e:
            print(f"❌ Processing failed: {str(e)}")
            raise
        finally:
            await self.client.close()
    
    def _estimate_time(self, total_rows: int) -> str:
        """Calculate estimated processing time"""
        batches = (total_rows + self.batch_size - 1) // self.batch_size
        seconds = batches * 90  # ~90 seconds per batch
        
        if seconds < 60:
            return f"{seconds} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
