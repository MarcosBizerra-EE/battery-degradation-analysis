import logging
from dataclasses import dataclass
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ==========================================
# 1. CONFIGURATION AND OBSERVABILITY
# ==========================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class BatteryConfig:
    """Data Contract and Pipeline Configurations."""
    # File paths
    input_file: str = "battery_48V_400cycles.csv"
    output_data: str = "processed_battery_data.csv"
    output_plot: str = "capacity_balance_chart.png"
    
    # Expected Schema Mapping
    col_time: str = 'Time(h)'
    col_current: str = 'Current(A)'
    col_voltage: str = 'Voltage(V)'
    col_cycle: str = 'Cycle'

# ==========================================
# 2. EXTRACT
# ==========================================
def extract_data(filepath: str) -> pd.DataFrame:
    """Reads raw data from the cycling equipment."""
    logger.info(f"Starting file read: {filepath}")
    try:
        return pd.read_csv(filepath)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise

# ==========================================
# 3. TRANSFORM - DATA QUALITY
# ==========================================
def enforce_schema_and_clean(df: pd.DataFrame, config: BatteryConfig) -> pd.DataFrame:
    """Applies the data contract, enforces numeric types, and removes corrupted rows."""
    logger.info("Applying data quality rules...")
    
    df_clean = df.copy()
    expected_columns = [config.col_time, config.col_current, config.col_voltage, config.col_cycle]
    
    # Ensure mandatory columns exist
    for col in expected_columns:
        if col not in df_clean.columns:
            logger.error(f"Missing mandatory column in schema: {col}")
            raise ValueError(f"Invalid Schema. Column {col} not found.")
        
        # Type coercion (Corrupted text becomes NaN)
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

    # Remove only rows that failed the validation contract for essential columns
    initial_rows = len(df_clean)
    df_clean = df_clean.dropna(subset=expected_columns).reset_index(drop=True)
    df_clean = df_clean.sort_values(by=config.col_time).reset_index(drop=True)
    
    logger.info(f"Data cleaning completed. Rows removed: {initial_rows - len(df_clean)}")
    return df_clean

# ==========================================
# 4. TRANSFORM - BUSINESS LOGIC (FEATURE ENGINEERING)
# ==========================================
def calculate_capacity_metrics(df: pd.DataFrame, config: BatteryConfig) -> pd.DataFrame:
    """Applies the domain-specific rule: Trapezoidal Integration of Current."""
    logger.info("Calculating capacity metrics using Trapezoidal Integration...")
    df_metrics = df.copy()
    
    # 1. Time delta (dt)
    df_metrics['dt_s'] = df_metrics[config.col_time].diff().fillna(0)
    
    # 2. Average Current (Trapezoidal Rule)
    df_metrics['current_avg'] = df_metrics[config.col_current].rolling(2).mean().fillna(df_metrics[config.col_current])
    
    # 3. Incremental Capacity (mAh)
    df_metrics['dCap_mAh'] = (df_metrics['current_avg'] * df_metrics['dt_s'] / 3600) * 1000
    
    # 4. Cumulative Net Capacity Balance
    df_metrics['Cumulative_Capacity_mAh'] = df_metrics['dCap_mAh'].cumsum()
    
    # 5. Charge/Discharge Status Classification
    df_metrics['Status'] = np.where(df_metrics['current_avg'] > 0, 'Charge', 
                           np.where(df_metrics['current_avg'] < 0, 'Discharge', 'Rest'))
    
    final_balance = df_metrics['Cumulative_Capacity_mAh'].iloc[-1]
    logger.info(f"Metrics calculated. Final net balance: {final_balance:.2f} mAh")
    return df_metrics

# ==========================================
# 5. LOAD AND VISUALIZE
# ==========================================
def plot_and_save_degradation(df: pd.DataFrame, config: BatteryConfig) -> None:
    """Generates the visual evidence chart and saves it to disk."""
    logger.info(f"Generating degradation chart: {config.output_plot}")
    
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Axis 1: Voltage
    color1 = 'tab:red'
    ax1.set_xlabel('Time (s)', fontweight='bold')
    ax1.set_ylabel('Voltage (V)', color=color1, fontweight='bold')
    ax1.plot(df[config.col_time], df[config.col_voltage], color=color1, alpha=0.7, label='Voltage (V)')
    ax1.tick_params(axis='y', labelcolor=color1)

    # Axis 2: Capacity
    ax2 = ax1.twinx()  
    color2 = 'tab:blue'
    ax2.set_ylabel('Cumulative Net Capacity (mAh)', color=color2, fontweight='bold')  
    ax2.plot(df[config.col_time], df['Cumulative_Capacity_mAh'], color=color2, linewidth=2, label='Energy Balance')
    ax2.tick_params(axis='y', labelcolor=color2)

    fig.suptitle('Charge Degradation: Voltage vs Net Capacity Balance', fontsize=14)
    fig.tight_layout()  
    plt.grid(True, linestyle='--', alpha=0.5)
    
    plt.savefig(config.output_plot)
    plt.close()
    logger.info("Chart saved successfully.")

def load_data_to_storage(df: pd.DataFrame, config: BatteryConfig) -> None:
    """Persists the processed data."""
    logger.info(f"Saving processed data to: {config.output_data}")
    df.to_csv(config.output_data, index=False)

# ==========================================
# 6. PIPELINE ORCHESTRATION (MAIN)
# ==========================================
def run_pipeline() -> None:
    """Main orchestrator function (Entrypoint)."""
    logger.info("--- STARTING BATTERY DATA PIPELINE ---")
    config = BatteryConfig()
    
    try:
        # Extract
        raw_df = extract_data(config.input_file)
        
        # Transform (Quality)
        clean_df = enforce_schema_and_clean(raw_df, config)
        
        # Transform (Business/Physics logic)
        final_df = calculate_capacity_metrics(clean_df, config)
        
        # Load / Visualize
        plot_and_save_degradation(final_df, config)
        load_data_to_storage(final_df, config)
        
        logger.info("--- PIPELINE EXECUTED SUCCESSFULLY ---")
        
    except Exception as e:
        logger.error(f"PIPELINE FAILURE: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()