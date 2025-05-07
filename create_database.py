# create_database.py
import sqlite3

# Database file path (update this path as needed)
db_path = r"path\to\your\compendium.db"

def create_database():
    """Create the Compound table in the SQLite database if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Define the Compound table schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Compound (
            CompoundID INTEGER PRIMARY KEY AUTOINCREMENT,
            Version INTEGER DEFAULT 1,
            CompoundName TEXT NOT NULL,
            SchedulingStatus TEXT CHECK(SchedulingStatus IN ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7')),
            RegisteredTradeName TEXT,
            CompoundType TEXT,
            AnimalsTreated TEXT,
            CurrentFormulations TEXT,
            PharmacologicalClassification TEXT,
            PharmacologicalAction TEXT,
            Indications TEXT,
            DosageDirections TEXT,
            WarningsPrecautions TEXT,
            ReferenceText TEXT,
            ApprovalStatus TEXT DEFAULT 'approval due',
            Approver TEXT,
            DateCreated DATE DEFAULT CURRENT_DATE,
            LastModified DATE DEFAULT CURRENT_DATE
        )
    ''')

    conn.commit()
    conn.close()
    print("Database and table created successfully.")

# Call the function
create_database()
