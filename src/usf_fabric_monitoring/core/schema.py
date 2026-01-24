"""
Data Definition and Semantic Model for USF Fabric Monitoring

This module defines the schema (DDL) for the monitoring datasets and 
represents the semantic model structure (tables, relationships, measures).
"""

from typing import Dict, List
from dataclasses import dataclass, field

# ==========================================
# 1. Data Definition (DDL) - Spark SQL / Delta
# ==========================================

DDL_ACTIVITIES_MASTER = """
CREATE TABLE IF NOT EXISTS activities_master (
    activity_id STRING,
    workspace_id STRING,
    workspace_name STRING,
    item_id STRING,
    item_name STRING,
    item_type STRING,
    activity_type STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    date DATE,
    hour INT,
    duration_seconds DOUBLE,
    duration_minutes DOUBLE,
    submitted_by STRING,
    created_by STRING,
    last_updated_by STRING,
    domain STRING,
    location STRING,
    object_url STRING,
    is_simulated BOOLEAN,
    failure_reason STRING,
    error_message STRING,
    error_code STRING,
    is_failed BOOLEAN,
    is_success BOOLEAN
) USING DELTA;
"""

DDL_KEY_MEASURABLES = """
CREATE TABLE IF NOT EXISTS key_measurables_summary (
    metric STRING,
    value DOUBLE,
    unit STRING,
    period STRING
) USING DELTA;
"""

DDL_USER_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS user_performance_analysis (
    user STRING,
    total_activities INT,
    failed_activities INT,
    success_rate_percent DOUBLE,
    total_duration_seconds DOUBLE,
    average_duration_seconds DOUBLE,
    daily_average_activities DOUBLE,
    risk_level STRING
) USING DELTA;
"""

DDL_DOMAIN_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS domain_performance_analysis (
    domain STRING,
    total_activities INT,
    failed_activities INT,
    success_rate_percent DOUBLE,
    total_duration_seconds DOUBLE,
    average_duration_seconds DOUBLE,
    percentage_of_total_activities DOUBLE
) USING DELTA;
"""

DDL_FAILURE_ANALYSIS = """
CREATE TABLE IF NOT EXISTS failure_analysis (
    item_id STRING,
    item_name STRING,
    item_type STRING,
    failure_count INT,
    severity STRING
) USING DELTA;
"""

DDL_DAILY_TRENDS = """
CREATE TABLE IF NOT EXISTS daily_trends_analysis (
    date DATE,
    total_activities INT,
    failed_activities INT,
    success_rate_percent DOUBLE,
    total_duration_seconds DOUBLE,
    average_duration_seconds DOUBLE
) USING DELTA;
"""

DDL_COMPUTE_ANALYSIS = """
CREATE TABLE IF NOT EXISTS compute_analysis (
    User STRING,
    Item_Name STRING,
    Item_Type STRING,
    Total_Runs INT,
    Successful_Runs INT,
    Failed_Runs INT,
    Unknown_In_Progress_Runs INT,
    Failure_Rate_Percent DOUBLE,
    Avg_Duration_Seconds DOUBLE,
    Total_Duration_Seconds DOUBLE,
    Last_Run_Time TIMESTAMP
) USING DELTA;
"""

ALL_DDLS = {
    "activities_master": DDL_ACTIVITIES_MASTER,
    "key_measurables": DDL_KEY_MEASURABLES,
    "user_performance": DDL_USER_PERFORMANCE,
    "domain_performance": DDL_DOMAIN_PERFORMANCE,
    "failure_analysis": DDL_FAILURE_ANALYSIS,
    "daily_trends": DDL_DAILY_TRENDS,
    "compute_analysis": DDL_COMPUTE_ANALYSIS
}

# ==========================================
# 2. Semantic Model Representation
# ==========================================

@dataclass
class Measure:
    name: str
    expression: str
    format_string: str = "#,##0"

@dataclass
class Relationship:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str = "Many-to-One"  # Many-to-One, One-to-One, etc.

@dataclass
class Table:
    name: str
    columns: List[str]
    measures: List[Measure] = field(default_factory=list)
    source_expression: str = ""

class FabricSemanticModel:
    """
    Represents the Semantic Model for Fabric Monitoring.
    This model is designed to be implemented in Power BI / Fabric Semantic Models.
    """

    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.relationships: List[Relationship] = []
        self._build_model()

    def _build_model(self):
        # --- Fact Table ---
        self.tables["Fact_Activities"] = Table(
            name="Fact_Activities",
            columns=[
                "activity_id", "workspace_id", "item_id", "date", "submitted_by",
                "status", "duration_seconds", "is_failed", "is_success"
            ],
            source_expression="SELECT * FROM activities_master",
            measures=[
                Measure("Total Activities", "COUNTROWS(Fact_Activities)"),
                Measure("Failed Activities", "CALCULATE(COUNTROWS(Fact_Activities), Fact_Activities[status] = 'Failed')"),
                Measure("Success Rate", "DIVIDE([Total Activities] - [Failed Activities], [Total Activities])", "0.0%"),
                Measure("Avg Duration (s)", "AVERAGE(Fact_Activities[duration_seconds])", "#,##0.00"),
                Measure("Total Duration (h)", "SUM(Fact_Activities[duration_seconds]) / 3600", "#,##0.00")
            ]
        )

        # --- Dimensions ---
        self.tables["Dim_Date"] = Table(
            name="Dim_Date",
            columns=["Date", "Year", "Month", "Day", "DayOfWeek"],
            source_expression="Calendar table generated from min/max of Fact_Activities[date]"
        )

        self.tables["Dim_User"] = Table(
            name="Dim_User",
            columns=["User", "Risk_Level"],
            source_expression="SELECT DISTINCT submitted_by as User FROM activities_master"
        )

        self.tables["Dim_Item"] = Table(
            name="Dim_Item",
            columns=["Item_Id", "Item_Name", "Item_Type", "Workspace_Name"],
            source_expression="SELECT DISTINCT item_id, item_name, item_type, workspace_name FROM activities_master"
        )

        self.tables["Dim_Workspace"] = Table(
            name="Dim_Workspace",
            columns=["Workspace_Id", "Workspace_Name", "Domain", "Location"],
            source_expression="SELECT DISTINCT workspace_id, workspace_name, domain, location FROM activities_master"
        )

        # --- Relationships ---
        self.relationships.append(Relationship("Fact_Activities", "date", "Dim_Date", "Date"))
        self.relationships.append(Relationship("Fact_Activities", "submitted_by", "Dim_User", "User"))
        self.relationships.append(Relationship("Fact_Activities", "item_id", "Dim_Item", "Item_Id"))
        self.relationships.append(Relationship("Fact_Activities", "workspace_id", "Dim_Workspace", "Workspace_Id"))

    def get_ddl(self) -> str:
        """Returns the DDL for all tables in the model"""
        return "\n".join(ALL_DDLS.values())

    def describe(self) -> str:
        """Returns a text description of the semantic model"""
        desc = "Fabric Monitoring Semantic Model\n"
        desc += "================================\n\n"

        desc += "Tables:\n"
        for name, table in self.tables.items():
            desc += f"- {name}\n"
            if table.measures:
                desc += "  Measures:\n"
                for m in table.measures:
                    desc += f"    - {m.name}: {m.expression}\n"

        desc += "\nRelationships:\n"
        for r in self.relationships:
            desc += f"- {r.from_table}[{r.from_column}] -> {r.to_table}[{r.to_column}] ({r.cardinality})\n"

        return desc

if __name__ == "__main__":
    model = FabricSemanticModel()
    print(model.describe())
