from setuptools import setup, find_packages

setup(
    name="batch-streaming",   # ชื่อ Framework ของคุณ
    version="1.0.3",                  # ระบุเวอร์ชัน (เมื่อมีการอัปเดต ให้มาเปลี่ยนเลขตรงนี้)
    description="Data Engineering Pipeline Framework for Databricks",
    author="Captain",
    packages=find_packages(where="src"),         # ค้นหาโฟลเดอร์ที่มี __init__.py อัตโนมัติ
    package_dir={"": "src"},
    install_requires=[
        # ระบุไลบรารีที่จำเป็นต้องใช้ (เช่น pandas)
        # หมายเหตุ: ไม่ต้องใส่ pyspark เพราะ Databricks มีให้อยู่แล้ว
    ]
)