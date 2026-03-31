from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="4a-simple_dynamic_task_mapping",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dynamic_mapping"]
)
def simple_dynamic_task_mapping():

    # 1. Task untuk menghasilkan daftar (list) data
    @task
    def get_items_to_process():
        """Menghasilkan list berisi angka yang akan diproses secara dinamis."""
        return [10, 20, 30, 40, 50]

    # 2. Task yang akan memproses SATU item
    @task
    def multiply_by_two(number: int):
        """Memproses item secara individual."""
        result = number * 2
        print(f"Angka {number} dikali 2 = {result}")
        return result

    # 3. Mendefinisikan alur dan pemetaan dinamis (Dynamic Mapping)
    items = get_items_to_process()
    
    # Kunci utamanya ada di .expand()
    # Airflow akan otomatis membuat 5 task "multiply_by_two" yang berjalan paralel
    mapped_tasks = multiply_by_two.expand(number=items)

# Inisialisasi DAG
simple_dynamic_task_mapping()