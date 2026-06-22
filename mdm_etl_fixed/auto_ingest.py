import os
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# NEW IMPORTS: Bring in the database and batch repository
from core.database import get_conn
from database.repositories import BatchMasterRepository
from pipelines.orchestrator import run_pipeline

# ─── Configuration ───
WATCH_DIR = "data"
PROCESSED_DIR = "data/processed"
FAILED_DIR = "data/failed"

# Ensure the directories exist
for directory in [WATCH_DIR, PROCESSED_DIR, FAILED_DIR]:
    os.makedirs(directory, exist_ok=True)

class ETLTriggerHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Ignore directory creations or non-CSV files
        if event.is_directory or not event.src_path.endswith('.csv'):
            return

        filepath = event.src_path
        filename = os.path.basename(filepath)
        print(f"\n[HOT FOLDER] 📥 Detected new file: {filename}")

        # Pause for 1 second to ensure the OS has completely finished writing the file
        time.sleep(1)

        # ─── Filename Parsing & Routing Logic ───
        name_without_ext = os.path.splitext(filename)[0]
        parts = name_without_ext.split('__')

        if len(parts) < 2:
            print(f"[HOT FOLDER] ❌ Invalid naming convention. Expected: clientID__useCaseName__timestamp.csv")
            self._move_file(filepath, FAILED_DIR, filename)
            return

        client_id = parts[0]
        use_case_name = parts[1]
        
        print(f"[HOT FOLDER] 🔍 Parsed Routing -> Client: '{client_id}', Use Case: '{use_case_name}'")

        try:
            print(f"[HOT FOLDER] 🚀 Registering batch and triggering ETL pipeline...")
            
            # 1. Connect to DB and register the batch FIRST
            conn = get_conn()
            batch_repo = BatchMasterRepository(conn)
            
            # This creates the row in batch_master and returns the valid UUID
            batch_id = batch_repo.create(
                client_name=client_id,
                use_case_name=use_case_name,
                run_by="auto_ingest_service"
            )
            
            # 2. Open the file and pass it to the orchestrator with the official batch_id
            with open(filepath, 'rb') as file_obj:
                report = run_pipeline(
                    batch_id=batch_id,
                    client_id=client_id,
                    use_case_id=use_case_name, 
                    config_id=None,            
                    csv_file=file_obj,
                    file_name=filename,
                )

            # Route the file based on the pipeline's success
            if report.get("error"):
                print(f"[HOT FOLDER] ❌ Pipeline failed: {report['error']}")
                self._move_file(filepath, FAILED_DIR, filename)
            else:
                print(f"[HOT FOLDER] ✅ Pipeline completed successfully!")
                self._move_file(filepath, PROCESSED_DIR, filename)

        except Exception as e:
            print(f"[HOT FOLDER] 💥 Critical error processing {filename}: {str(e)}")
            self._move_file(filepath, FAILED_DIR, filename)

    def _move_file(self, src_path, dest_dir, filename):
        """Safely moves the file to prevent it from being processed twice."""
        dest_path = os.path.join(dest_dir, filename)
        if os.path.exists(dest_path):
            os.remove(dest_path)
        shutil.move(src_path, dest_path)
        print(f"[HOT FOLDER] 📁 Moved {filename} to {dest_dir}/")


if __name__ == "__main__":
    event_handler = ETLTriggerHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()
    
    print(f"👀 Dynamic Routing Service Started. Watching '{WATCH_DIR}/'...")
    print("Expected format: client_id__use_case_name__timestamp.csv")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nService Stopped.")
    
    observer.join()