# Power BI Dashboard for USF Fabric Monitoring

This directory contains all resources needed to create and deploy a Power BI dashboard for the USF Fabric Monitoring star schema.

## Directory Structure

```
powerbi/
├── README.md                           # This file
├── deploy_powerbi_report.py            # Deployment script using Fabric REST APIs
├── report_layout.json                  # Visual layout specification (4 pages)
├── measures/
│   └── fabric_monitoring_measures.dax  # All DAX measures (50+ measures)
├── themes/
│   └── usf_fabric_monitoring_theme.json # Custom Power BI theme
└── deployment/
    └── measures_*.dax                  # Generated deployment scripts
```

## Quick Start

### Option 1: Fabric Direct Lake (Recommended)

1. **Run the Star Schema Builder notebook** with `WRITE_TO_DELTA_TABLES = True`
2. **In Fabric Lakehouse** → SQL Endpoint → New semantic model
3. **Select tables**: all `dim_*` and `fact_*` tables
4. **Create relationships** (see below)
5. **Add DAX measures** from `measures/fabric_monitoring_measures.dax`
6. **Create report** → Click "Create report" from semantic model
7. **Apply theme** → Import `themes/usf_fabric_monitoring_theme.json`

### Option 2: Power BI Desktop

1. **Open Power BI Desktop**
2. **Get Data** → Power BI datasets → Connect to your semantic model
3. **Model View** → Add all DAX measures from the `.dax` file
4. **Report View** → Create visuals following `report_layout.json`
5. **Apply theme** → View → Themes → Browse → Select theme file
6. **Publish** to your Fabric workspace

### Option 3: Deployment Script

```bash
# Activate conda environment first!
conda activate fabric-monitoring

# Run deployment script
python powerbi/deploy_powerbi_report.py \
    --workspace "Your Workspace Name" \
    --dataset "USF Fabric Monitoring Model" \
    --report "USF Fabric Monitoring Dashboard"

# List available measures
python powerbi/deploy_powerbi_report.py --list-measures
```

## Semantic Model Setup

### Tables Required

| Table | Type | Description |
|-------|------|-------------|
| `fact_activity` | Fact | Main activity records (1.28M+ rows) |
| `fact_daily_metrics` | Fact | Pre-aggregated daily metrics |
| `dim_date` | Dimension | Calendar dates |
| `dim_time` | Dimension | Hours and time periods |
| `dim_workspace` | Dimension | Workspace names and environments |
| `dim_item` | Dimension | Fabric items (notebooks, pipelines, etc.) |
| `dim_user` | Dimension | User principal names |
| `dim_activity_type` | Dimension | Activity types and categories |
| `dim_status` | Dimension | Status codes (Succeeded, Failed) |

### Relationships

Create these relationships in your semantic model:

```
fact_activity[date_sk] → dim_date[date_sk] (Many-to-One)
fact_activity[time_sk] → dim_time[time_sk] (Many-to-One)
fact_activity[workspace_sk] → dim_workspace[workspace_sk] (Many-to-One)
fact_activity[item_sk] → dim_item[item_sk] (Many-to-One)
fact_activity[user_sk] → dim_user[user_sk] (Many-to-One)
fact_activity[activity_type_sk] → dim_activity_type[activity_type_sk] (Many-to-One)
fact_activity[status_sk] → dim_status[status_sk] (Many-to-One)
```

### Mark as Date Table

In the semantic model, mark `dim_date` as a date table:
- Table: `dim_date`
- Date column: `full_date`

This enables time intelligence functions like `DATESMTD`, `PREVIOUSMONTH`, etc.

## DAX Measures Overview

The `measures/fabric_monitoring_measures.dax` file contains 50+ measures organized into sections:

| Section | Measures | Description |
|---------|----------|-------------|
| Core KPIs | 8 | Total Activities, Failed, Success Rate, Active Users/Workspaces |
| Duration | 7 | Total/Avg/Max Duration, Long Running Activities |
| Time Comparison | 8 | Today, Yesterday, Last 7/28 Days, DoD/WoW Change |
| Time Intelligence | 5 | MTD, YTD, MoM comparisons |
| Environment | 6 | PRD/UAT/TEST/DEV Activities and Failure Rates |
| Activity Type | 6 | Pipeline, Notebook, Dataflow, etc. |
| User Analysis | 3 | Activities per User, User Rank |
| Workspace Analysis | 3 | Activities per Workspace, Workspace Rank |
| Hourly Analysis | 3 | Business Hours, Off-Hours, Peak Hour |
| Conditional Formatting | 4 | Color measures for RAG status |
| Sparklines | 1 | Daily Activity Trend data |

### Key Measure: Total Activities

**IMPORTANT**: Always use `SUM(fact_activity[record_count])` instead of `COUNT(activity_id)`:

```dax
// ✅ CORRECT
Total Activities = SUM(fact_activity[record_count])

// ❌ WRONG - 99% of activity_ids are NULL
Total Activities = COUNT(fact_activity[activity_id])
```

This is because audit log entries (ReadArtifact, ViewReport, etc.) don't have activity IDs - only job history entries do.

## Report Pages

The dashboard has 4 pages as defined in `report_layout.json`:

### Page 1: Executive Overview
- KPI Cards: Total Activities, Failed, Success Rate, Active Users, Workspaces
- Activity Trend line chart (Total + Failed over time)
- Activity by Type bar chart (Top 15)
- Activity by Workspace bar chart (Top 10)

### Page 2: Operational Details
- Activity by Hour of Day
- Activity by Day of Week
- Top Users table
- Top Items table

### Page 3: Failure Analysis
- Failed Activities card
- Failure Rate card
- Failure Trend area chart
- Failures by Type donut chart
- Failures by Workspace bar chart
- Failure Details matrix (Type × Environment)

### Page 4: Duration Analysis
- Duration KPI cards
- Duration by Activity Type
- Average Duration by Type
- Long Running Activities table

## Theme Customization

The `themes/usf_fabric_monitoring_theme.json` includes:

- **Data Colors**: Microsoft Fluent UI palette (blue, red, green, yellow, purple)
- **RAG Colors**: Green (#107C10), Amber (#FFB900), Red (#E81123)
- **Typography**: Segoe UI family
- **Visual Styles**: Cards, charts, tables, slicers all pre-configured

### Applying the Theme

**Power BI Desktop**:
View → Themes → Browse for themes → Select `usf_fabric_monitoring_theme.json`

**Power BI Service**:
1. Open report in Edit mode
2. View → Themes → Browse for themes
3. Upload the JSON file

## Troubleshooting

### "Dataset not found"
- Ensure the semantic model is created from the star schema tables
- Check the semantic model name matches what you're passing to the script

### Measures returning BLANK
- Verify relationships are created correctly
- Check that `dim_date` is marked as a date table
- Ensure the filter context is correct

### Wrong activity counts
- Use `SUM(record_count)` not `COUNT(activity_id)`
- Check for filter context from slicers

### Authentication errors
- Set environment variables: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`
- Or use `.env` file in the project root
- Service Principal needs Power BI Admin or appropriate dataset permissions

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.3.8 | 2025-12-18 | Initial release with 4 pages, 50+ measures |

## Author

- **Author**: Sanmi Ibitoye
- **Email**: Sanmi.Ibitoye@leit.ltd
