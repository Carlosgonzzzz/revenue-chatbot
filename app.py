import streamlit as st
import anthropic
import os
from dotenv import load_dotenv
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import random
import string

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Revenue Intelligence Platform",
    page_icon="📊",
    layout="wide"
)

# Minimal CSS - just avatar size
st.markdown("""
<style>
    .stChatMessage img {
        width: 60px !important;
        height: 60px !important;
        border-radius: 50% !important;
    }
    .alert-card {
        padding: 20px;
        border-radius: 8px;
        border-left: 4px solid;
        margin: 10px 0;
    }
    .alert-critical {
        background-color: #2d1e1e;
        border-color: #ff4444;
    }
    .alert-warning {
        background-color: #2d2a1e;
        border-color: #ffaa00;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "demo_mode" not in st.session_state:
    st.session_state.demo_mode = True

if "live_mode_questions" not in st.session_state:
    st.session_state.live_mode_questions = 0

if "show_investigation" not in st.session_state:
    st.session_state.show_investigation = {}

# Clear old investigation data on app restart to prevent type errors
if "app_version" not in st.session_state:
    st.session_state.show_investigation = {}
    st.session_state.app_version = "v2.0"

if "show_investigation" not in st.session_state:
    st.session_state.show_investigation = {}

# ============================================
# HELPER FUNCTIONS
# ============================================

def query_database(query):
    """Execute SQL query and return results"""
    try:
        conn = sqlite3.connect('revenue_data.db')
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return None

def get_executive_metrics():
    """Calculate all executive KPIs"""
    metrics = {}
    
    # Total won deals and revenue (using all available data)
    won_query = """
        SELECT 
            COUNT(*) as total_won,
            SUM(close_value) as total_revenue
        FROM sales_pipeline
        WHERE deal_stage = 'Won' AND close_date IS NOT NULL
    """
    won_result = query_database(won_query)
    if won_result:
        total_won, total_revenue = won_result[0]
        metrics['won_deals'] = int(total_won or 0)
        metrics['qtd_revenue'] = int(total_revenue or 0)  # Use total as "YTD"
        
        # For projections, calculate based on data's actual time period
        date_range_query = """
            SELECT 
                MIN(close_date) as first_date,
                MAX(close_date) as last_date,
                JULIANDAY(MAX(close_date)) - JULIANDAY(MIN(close_date)) as days_span
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
        """
        date_result = query_database(date_range_query)
        if date_result and date_result[0][2]:
            days_span = date_result[0][2]
            # Monthly revenue average
            monthly_avg = (total_revenue or 0) / (days_span / 30)
            # EOQ = 3 months projection
            metrics['eoq_projection'] = int(monthly_avg * 3)
            # EOY = 12 months projection
            metrics['quarter_projection'] = int(monthly_avg * 12)
        else:
            metrics['eoq_projection'] = 0
            metrics['quarter_projection'] = 0
    
    # Win rate
    win_rate_query = """
        SELECT 
            SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        FROM sales_pipeline
        WHERE deal_stage IN ('Won', 'Lost')
    """
    win_rate_result = query_database(win_rate_query)
    if win_rate_result:
        metrics['win_rate'] = round(win_rate_result[0][0] or 0, 1)
    
    # Pipeline health - all open pipeline vs monthly quota
    pipeline_query = """
        WITH avg_won_deal AS (
            SELECT AVG(close_value) as avg_value
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_value > 0
        )
        SELECT 
            COUNT(*) as open_deals,
            (SELECT avg_value FROM avg_won_deal) as avg_deal_value
        FROM sales_pipeline
        WHERE deal_stage = 'Engaging'
    """
    pipeline_result = query_database(pipeline_query)
    if pipeline_result and pipeline_result[0]:
        open_deals, avg_value = pipeline_result[0]
        open_deals = open_deals or 0
        avg_value = avg_value or 0
        
        # Estimated pipeline value = # of deals × avg won deal size
        estimated_value = open_deals * avg_value
        
        # Monthly quota (from historical data - ~10 months)
        monthly_quota = metrics.get('qtd_revenue', 0) / 10
        coverage = estimated_value / monthly_quota if monthly_quota > 0 else 0
        
        metrics['pipeline_health'] = round(coverage, 1)
        metrics['pipeline_value'] = int(estimated_value)
    else:
        metrics['pipeline_health'] = 0
        metrics['pipeline_value'] = 0
    
    # Average deal size
    avg_deal_query = """
        SELECT AVG(close_value)
        FROM sales_pipeline
        WHERE deal_stage = 'Won' AND close_value > 0
    """
    avg_result = query_database(avg_deal_query)
    if avg_result:
        metrics['avg_deal_size'] = int(avg_result[0][0] or 0)
    
    # Top product
    top_product_query = """
        SELECT product, SUM(close_value) as revenue
        FROM sales_pipeline
        WHERE deal_stage = 'Won'
        GROUP BY product
        ORDER BY revenue DESC
        LIMIT 1
    """
    top_product_result = query_database(top_product_query)
    if top_product_result:
        metrics['top_region'] = top_product_result[0][0]
        metrics['top_region_revenue'] = int(top_product_result[0][1] or 0)
    
    return metrics

def detect_alerts():
    """Detect business problems that need attention"""
    alerts = []
    
    # Alert 1: Win rate declining (quarter over quarter)
    win_rate_trend = query_database("""
        SELECT 
            CASE 
                WHEN strftime('%m', close_date) IN ('01','02','03') THEN 'Q1'
                WHEN strftime('%m', close_date) IN ('04','05','06') THEN 'Q2'
                WHEN strftime('%m', close_date) IN ('07','08','09') THEN 'Q3'
                ELSE 'Q4'
            END as quarter,
            SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM sales_pipeline
        WHERE deal_stage IN ('Won', 'Lost')
        AND close_date IS NOT NULL
        GROUP BY quarter
        ORDER BY quarter DESC
        LIMIT 4
    """)
    
    if win_rate_trend and len(win_rate_trend) >= 2:
        current_wr = win_rate_trend[0][1]
        prev_wr = win_rate_trend[1][1]
        if current_wr < prev_wr - 3:  # 3% drop
            alerts.append({
                'severity': 'critical',
                'title': 'Win Rate Declining',
                'message': f'Win rate dropped from {prev_wr:.1f}% to {current_wr:.1f}% ({prev_wr - current_wr:.1f}% decline) quarter-over-quarter',
                'type': 'win_rate_decline'
            })
    
    # Alert 2: Pipeline coverage (adjusted threshold)
    metrics = get_executive_metrics()
    pipeline_health = metrics.get('pipeline_health', 0)
    if pipeline_health < 4:  # Below 4x coverage
        severity = 'critical' if pipeline_health < 2.5 else 'warning'
        alerts.append({
            'severity': severity,
            'title': 'Pipeline Coverage Needs Attention',
            'message': f'Pipeline coverage at {pipeline_health}x - target is 4x+ for healthy growth',
            'type': 'pipeline_coverage'
        })
    
    # Alert 3: Deal velocity slowing
    velocity_trend = query_database("""
        SELECT 
            strftime('%Y-%m', close_date) as month,
            AVG(JULIANDAY(close_date) - JULIANDAY(engage_date)) as avg_days
        FROM sales_pipeline
        WHERE deal_stage = 'Won'
        AND close_date IS NOT NULL
        AND engage_date IS NOT NULL
        GROUP BY month
        ORDER BY month DESC
        LIMIT 3
    """)
    
    if velocity_trend and len(velocity_trend) >= 2:
        current_cycle = velocity_trend[0][1]
        prev_cycle = velocity_trend[1][1]
        if current_cycle > prev_cycle * 1.15:  # 15% increase
            alerts.append({
                'severity': 'warning',
                'title': 'Sales Cycle Lengthening',
                'message': f'Average sales cycle increased from {int(prev_cycle)} to {int(current_cycle)} days ({int(current_cycle - prev_cycle)} day increase)',
                'type': 'sales_cycle'
            })
    
    # Alert 4: Revenue concentration risk - fixed to use above-average metric
    rep_concentration = query_database("""
        WITH rep_revenue AS (
            SELECT 
                sales_agent,
                SUM(close_value) as revenue
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
        ),
        avg_revenue AS (
            SELECT AVG(revenue) as avg_rev FROM rep_revenue
        )
        SELECT 
            COUNT(*) as total_reps,
            SUM(CASE WHEN revenue > (SELECT avg_rev FROM avg_revenue) THEN 1 ELSE 0 END) as above_avg_reps
        FROM rep_revenue
    """)
    
    if rep_concentration:
        total_reps, above_avg_reps = rep_concentration[0]
        pct_above_avg = (above_avg_reps / total_reps * 100) if total_reps > 0 else 0
        
        if pct_above_avg < 40:  # Less than 40% above average
            alerts.append({
                'severity': 'critical',
                'title': 'Revenue Concentration Risk',
                'message': f'Only {above_avg_reps}/{total_reps} reps performing above average - majority underperforming',
                'type': 'rep_concentration'
            })
    
    # Alert 5: Product performance imbalance
    product_performance = query_database("""
        SELECT 
            product,
            COUNT(*) as deals,
            SUM(close_value) as revenue,
            SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                NULLIF(SUM(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 ELSE 0 END), 0) as win_rate
        FROM sales_pipeline
        WHERE product IS NOT NULL
        GROUP BY product
        HAVING deals > 50
        ORDER BY revenue DESC
    """)
    
    if product_performance and len(product_performance) >= 2:
        top_product_revenue = product_performance[0][2]
        second_product_revenue = product_performance[1][2]
        
        if top_product_revenue > second_product_revenue * 3:  # 3x imbalance
            alerts.append({
                'severity': 'warning',
                'title': 'Product Mix Imbalance',
                'message': f'Top product ({product_performance[0][0]}) generates 3x+ more revenue than others - diversification opportunity',
                'type': 'product_imbalance'
            })
    
    # Alert 6: Churn risk - deals stuck in pipeline
    # Use the latest date in the dataset as "now" instead of actual current date
    latest_date_query = query_database("SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL")
    reference_date = latest_date_query[0][0] if latest_date_query and latest_date_query[0][0] else '2017-12-31'
    
    stuck_deals = query_database(f"""
        SELECT COUNT(*) as stuck_count,
               SUM(close_value) as stuck_value
        FROM sales_pipeline
        WHERE deal_stage = 'Engaging'
        AND engage_date IS NOT NULL
        AND JULIANDAY('{reference_date}') - JULIANDAY(engage_date) > 90
    """)
    
    if stuck_deals and stuck_deals[0][0] > 0:
        stuck_count = stuck_deals[0][0]
        stuck_value = stuck_deals[0][1] or 0
        
        # Estimate value for deals without close_value
        if stuck_value == 0:
            avg_deal_query = query_database("SELECT AVG(close_value) FROM sales_pipeline WHERE deal_stage = 'Won'")
            avg_deal_val = avg_deal_query[0][0] if avg_deal_query else 2361
            stuck_value = stuck_count * avg_deal_val
        
        # Check if this is > 50% of pipeline (increased threshold to reduce false positives)
        total_pipeline = query_database("SELECT COUNT(*) FROM sales_pipeline WHERE deal_stage = 'Engaging'")
        if total_pipeline and total_pipeline[0][0] > 0:
            stuck_percentage = (stuck_count / total_pipeline[0][0])
            if stuck_percentage > 0.5:  # Only alert if >50% of pipeline is stuck
                alerts.append({
                    'severity': 'warning',
                    'title': 'High Churn Risk - Stale Pipeline',
                    'message': f'{stuck_count} deals stuck >90 days (${int(stuck_value):,} at risk) - {(stuck_percentage * 100):.0f}% of pipeline',
                    'type': 'churn_risk'
                })
    
    # Alert 7: Territory imbalance
    territory_balance = query_database("""
        SELECT 
            sales_agent,
            COUNT(CASE WHEN deal_stage = 'Engaging' THEN 1 END) as active_deals
        FROM sales_pipeline
        GROUP BY sales_agent
        HAVING active_deals > 0
        ORDER BY active_deals DESC
    """)
    
    if territory_balance and len(territory_balance) >= 5:
        top_load = territory_balance[0][1]
        bottom_load = territory_balance[-1][1]
        
        # If top rep has 3x+ more deals than bottom
        if top_load > bottom_load * 3:
            alerts.append({
                'severity': 'warning',
                'title': 'Territory Load Imbalance',
                'message': f'Uneven deal distribution: top rep has {top_load} active deals vs {bottom_load} for lowest - 3x+ imbalance',
                'type': 'territory_imbalance'
            })
    
    # Alert 8: New rep ramp issues
    new_rep_ramp = query_database("""
        WITH rep_first_deal AS (
            SELECT 
                sales_agent,
                MIN(engage_date) as first_engage,
                MIN(CASE WHEN deal_stage = 'Won' THEN close_date END) as first_win,
                COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) as total_wins
            FROM sales_pipeline
            GROUP BY sales_agent
        )
        SELECT 
            sales_agent,
            JULIANDAY(first_win) - JULIANDAY(first_engage) as days_to_first_win,
            total_wins
        FROM rep_first_deal
        WHERE first_win IS NOT NULL
        AND JULIANDAY('now') - JULIANDAY(first_engage) < 180  -- Reps in first 6 months
    """)
    
    if new_rep_ramp:
        avg_ramp = sum(r[1] for r in new_rep_ramp) / len(new_rep_ramp)
        slow_ramp_reps = [r for r in new_rep_ramp if r[1] > avg_ramp * 1.5]
        
        if len(slow_ramp_reps) > 0:
            alerts.append({
                'severity': 'warning',
                'title': 'New Rep Ramp Time Issues',
                'message': f'{len(slow_ramp_reps)} new reps took 50%+ longer than average to close first deal (avg: {int(avg_ramp)} days)',
                'type': 'slow_ramp'
            })
    
    # Alert 9: Discount frequency (deals closing below average)
    discount_analysis = query_database("""
        WITH avg_deal AS (
            SELECT AVG(close_value) as avg_val FROM sales_pipeline WHERE deal_stage = 'Won'
        )
        SELECT 
            COUNT(CASE WHEN close_value < (SELECT avg_val FROM avg_deal) * 0.8 THEN 1 END) as discounted_deals,
            COUNT(*) as total_deals,
            COUNT(CASE WHEN close_value < (SELECT avg_val FROM avg_deal) * 0.8 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as discount_rate
        FROM sales_pipeline
        WHERE deal_stage = 'Won'
        AND close_date >= date('now', '-90 days')
    """)
    
    if discount_analysis and len(discount_analysis) > 0 and len(discount_analysis[0]) >= 3:
        discounted, total, rate = discount_analysis[0]
        if rate is not None and rate > 30:  # >30% of deals heavily discounted
            alerts.append({
                'severity': 'critical',
                'title': 'High Discount Frequency',
                'message': f'{int(rate):.0f}% of recent deals closed 20%+ below average price ({int(discounted)}/{int(total)} deals) - margin pressure',
                'type': 'discount_frequency'
            })
    
    # Alert 10: Seasonal performance drop
    seasonal_trend = query_database("""
        SELECT 
            CASE 
                WHEN strftime('%m', close_date) IN ('01','02','03') THEN 'Q1'
                WHEN strftime('%m', close_date) IN ('04','05','06') THEN 'Q2'
                WHEN strftime('%m', close_date) IN ('07','08','09') THEN 'Q3'
                ELSE 'Q4'
            END as quarter,
            strftime('%Y', close_date) as year,
            SUM(close_value) as revenue,
            COUNT(*) as deals
        FROM sales_pipeline
        WHERE deal_stage = 'Won'
        GROUP BY quarter, year
        ORDER BY year DESC, quarter DESC
        LIMIT 4
    """)
    
    if seasonal_trend and len(seasonal_trend) >= 2:
        current_rev = seasonal_trend[0][2]
        prev_rev = seasonal_trend[1][2]
        
        if current_rev < prev_rev * 0.75:  # 25%+ drop from previous quarter
            alerts.append({
                'severity': 'critical',
                'title': 'Seasonal Performance Drop',
                'message': f'Revenue down {((prev_rev - current_rev) / prev_rev * 100):.0f}% vs previous quarter (${int(current_rev):,} vs ${int(prev_rev):,})',
                'type': 'seasonal_drop'
            })
    
    return alerts

def investigate_alert(alert_type):
    """Run diagnostic tree for specific alert"""
    if alert_type == 'win_rate_decline':
        # Deep analysis of win rate
        stage_analysis = query_database("""
            SELECT deal_stage, COUNT(*) as count
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
            GROUP BY deal_stage
        """)
        
        # Analyze loss reasons by product
        product_loss = query_database("""
            SELECT 
                product,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN deal_stage = 'Lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                    COUNT(*) as win_rate
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
            GROUP BY product
            ORDER BY win_rate ASC
            LIMIT 3
        """)
        
        findings = [
            f"Won deals: {stage_analysis[0][1] if len(stage_analysis) > 0 else 0:,}",
            f"Lost deals: {stage_analysis[1][1] if len(stage_analysis) > 1 else 0:,}",
            "",
            "Products with lowest win rates:"
        ]
        for product, won, lost, wr in product_loss:
            findings.append(f"{product}: {wr:.1f}% win rate ({int(won)} won, {int(lost)} lost)")
        
        return {
            'root_cause': 'Win rate decline - poorest performing products identified',
            'findings': findings,
            'recommendation_prompt': 'Win rate declining - analyze competitive losses and product positioning'
        }
    
    elif alert_type == 'pipeline_coverage':
        pipeline_query = """
            WITH avg_won_deal AS (
                SELECT AVG(close_value) as avg_value
                FROM sales_pipeline
                WHERE deal_stage = 'Won' AND close_value > 0
            )
            SELECT 
                COUNT(*) as open_deals,
                (SELECT avg_value FROM avg_won_deal) as avg_deal_value,
                COUNT(*) * (SELECT avg_value FROM avg_won_deal) as estimated_value
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
        """
        pipeline_result = query_database(pipeline_query)
        open_deals = pipeline_result[0][0] if pipeline_result else 0
        avg_deal_value = pipeline_result[0][1] if pipeline_result else 0
        estimated_value = int(pipeline_result[0][2] if pipeline_result else 0)
        
        # Calculate needed pipeline
        monthly_quota = 1000000  # Assumed monthly target
        needed_coverage = monthly_quota * 4
        gap = needed_coverage - estimated_value
        
        return {
            'root_cause': 'Insufficient pipeline to hit targets',
            'findings': [
                f"Current pipeline: {open_deals:,} deals",
                f"Estimated value: ${estimated_value:,}",
                f"Target coverage: ${needed_coverage:,} (4x monthly quota)",
                f"Gap: ${gap:,} shortfall",
                f"Need {int(gap/avg_deal_value):,} more qualified opportunities"
            ],
            'recommendation_prompt': 'Pipeline coverage low - need strategies to increase top-of-funnel activity'
        }
    
    elif alert_type == 'sales_cycle':
        # Deep dive into what changed to cause slowdown
        cycle_trend = query_database("""
            SELECT 
                strftime('%Y-%m', close_date) as month,
                AVG(JULIANDAY(close_date) - JULIANDAY(engage_date)) as avg_days,
                AVG(close_value) as avg_deal_size,
                COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
            GROUP BY month
            ORDER BY month DESC
            LIMIT 3
        """)
        
        # Compare deal sizes between fast and slow months
        if cycle_trend and len(cycle_trend) >= 2:
            latest_month, latest_cycle, latest_size, latest_count = cycle_trend[0]
            prev_month, prev_cycle, prev_size, prev_count = cycle_trend[1]
            
            cycle_increase = latest_cycle - prev_cycle
            size_increase = ((latest_size - prev_size) / prev_size * 100) if prev_size > 0 else 0
            
            findings = [
                "Sales cycle analysis by month:",
                f"{latest_month}: {int(latest_cycle)} days average (${int(latest_size):,} avg deal)",
                f"{prev_month}: {int(prev_cycle)} days average (${int(prev_size):,} avg deal)",
                "Root cause identified: " + str(int(cycle_increase)) + " day increase driven by:"
            ]
            
            # Determine primary driver
            if abs(size_increase) > 20:
                findings.append(f"1. Deal size increased {size_increase:.1f}% → Larger deals = more stakeholders/approvals")
                findings.append("2. Moving upmarket requires longer procurement cycles")
            else:
                findings.append("1. Process friction introduced (not deal size related)")
                findings.append("2. Likely causes: Added approval layers, extended legal review, or resource constraints")
            
            return {
                'root_cause': f'Sales velocity slowing - {int(cycle_increase)} day increase in latest period',
                'findings': findings,
                'recommendation_prompt': 'Sales cycle lengthening - identify bottlenecks and acceleration strategies'
            }
        
        return {
            'root_cause': 'Sales velocity slowing',
            'findings': ["Insufficient data for detailed analysis"],
            'recommendation_prompt': 'Sales cycle lengthening - identify bottlenecks and acceleration strategies'
        }
    
    elif alert_type == 'rep_concentration':
        # Deep analysis: What separates top from bottom performers?
        rep_analysis = query_database("""
            WITH rep_metrics AS (
                SELECT 
                    sales_agent,
                    SUM(close_value) as revenue,
                    COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) as won_deals,
                    COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 END) as total_attempts,
                    SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 END), 0) as win_rate,
                    AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) as avg_deal_size
                FROM sales_pipeline
                GROUP BY sales_agent
            ),
            avg_metrics AS (
                SELECT 
                    AVG(revenue) as avg_revenue,
                    AVG(win_rate) as avg_win_rate,
                    AVG(avg_deal_size) as avg_deal_size
                FROM rep_metrics
            )
            SELECT 
                r.sales_agent,
                r.revenue,
                r.won_deals,
                r.win_rate,
                r.avg_deal_size,
                (SELECT avg_revenue FROM avg_metrics) as avg_rev,
                (SELECT avg_win_rate FROM avg_metrics) as avg_wr,
                (SELECT avg_deal_size FROM avg_metrics) as avg_size,
                r.revenue - (SELECT avg_revenue FROM avg_metrics) as revenue_gap,
                r.win_rate - (SELECT avg_win_rate FROM avg_metrics) as wr_gap,
                r.avg_deal_size - (SELECT avg_deal_size FROM avg_metrics) as size_gap
            FROM rep_metrics r
            WHERE r.revenue < (SELECT avg_revenue FROM avg_metrics)
            ORDER BY r.revenue ASC
        """)
        
        if rep_analysis:
            avg_revenue = rep_analysis[0][5]
            avg_wr = rep_analysis[0][6]
            avg_size = rep_analysis[0][7]
            
            # Analyze what's the biggest gap
            total_revenue_gap = sum(abs(r[8]) for r in rep_analysis)
            
            # Find primary differentiator
            underperformers_by_impact = []
            for agent, rev, deals, wr, size, _, _, _, rev_gap, wr_gap, size_gap in rep_analysis:
                # Calculate business impact
                impact = abs(rev_gap)
                
                # Determine primary issue
                if abs(wr_gap) > 10:  # Win rate 10%+ below average
                    issue = f"Win rate issue ({wr:.1f}%)"
                elif abs(size_gap) > 500:  # Deal size $500+ below average
                    rep_size = f"${int(size):,}"
                    issue = f"Deal size issue ({rep_size})"
                else:
                    issue = f"Activity issue ({int(deals)} deals)"
                
                underperformers_by_impact.append({
                    'agent': agent,
                    'revenue': rev,
                    'gap': rev_gap,
                    'impact': impact,
                    'issue': issue
                })
            
            # Sort by business impact
            underperformers_by_impact.sort(key=lambda x: x['impact'], reverse=True)
            
            findings = [
                f"Average revenue per rep: ${int(avg_revenue):,}",
                f"Total revenue at risk: ${int(total_revenue_gap):,}",
                "Underperformers by business impact:"
            ]
            
            for i, rep in enumerate(underperformers_by_impact[:10], 1):
                findings.append(f"{i}. {rep['agent']}: ${int(rep['revenue']):,} (${int(abs(rep['gap'])):,} below avg) - {rep['issue']}")
            
            return {
                'root_cause': f'{len(rep_analysis)} reps below average - ${int(total_revenue_gap):,} revenue at risk',
                'findings': findings,
                'recommendation_prompt': 'Revenue concentration - develop targeted coaching based on specific performance gaps'
            }
        
        return {
            'root_cause': 'Revenue concentrated in few reps',
            'findings': ["Insufficient data for detailed analysis"],
            'recommendation_prompt': 'Heavy revenue concentration - develop strategies for broader team performance'
        }
    
    elif alert_type == 'product_imbalance':
        product_breakdown = query_database("""
            SELECT 
                product,
                COUNT(*) as deals,
                SUM(close_value) as revenue,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                    NULLIF(SUM(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 ELSE 0 END), 0) as win_rate
            FROM sales_pipeline
            GROUP BY product
            ORDER BY revenue DESC
        """)
        
        total_revenue = sum(r[2] for r in product_breakdown if r[2])
        
        findings = ["Product performance (% of total revenue):"]
        for product, deals, revenue, win_rate in product_breakdown[:5]:
            pct = (revenue / total_revenue * 100) if total_revenue > 0 else 0
            findings.append(f"{product}: ${int(revenue):,} ({pct:.1f}% of revenue, {int(deals)} deals, {win_rate:.1f}% win rate)")
        
        return {
            'root_cause': 'Revenue heavily concentrated in one product line',
            'findings': findings,
            'recommendation_prompt': 'Product mix imbalance - develop strategies to diversify revenue streams'
        }
    
    elif alert_type == 'churn_risk':
        # Get reference date from dataset (latest close date)
        latest_date_query = query_database("SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL")
        reference_date = latest_date_query[0][0] if latest_date_query and latest_date_query[0][0] else '2017-12-31'
        
        # Analyze WHY deals are stuck
        stuck_analysis = query_database(f"""
            WITH avg_won_deal AS (
                SELECT AVG(close_value) as avg_val 
                FROM sales_pipeline 
                WHERE deal_stage = 'Won' AND close_value > 0
            ),
            stuck_deals AS (
                SELECT 
                    sales_agent,
                    product,
                    COALESCE(close_value, (SELECT avg_val FROM avg_won_deal)) as estimated_value,
                    JULIANDAY('{reference_date}') - JULIANDAY(engage_date) as days_stuck
                FROM sales_pipeline
                WHERE deal_stage = 'Engaging'
                AND engage_date IS NOT NULL
                AND JULIANDAY('{reference_date}') - JULIANDAY(engage_date) > 90
            )
            SELECT 
                sales_agent,
                COUNT(*) as stuck_count,
                AVG(days_stuck) as avg_days_stuck,
                SUM(estimated_value) as value_at_risk
            FROM stuck_deals
            GROUP BY sales_agent
            ORDER BY stuck_count DESC
            LIMIT 5
        """)
        
        # Check if stuck deals are larger than average (complexity issue)
        size_check = query_database(f"""
            WITH avg_won_deal AS (
                SELECT AVG(close_value) as avg_val 
                FROM sales_pipeline 
                WHERE deal_stage = 'Won' AND close_value > 0
            )
            SELECT AVG(COALESCE(close_value, (SELECT avg_val FROM avg_won_deal))) as avg_stuck
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
            AND JULIANDAY('{reference_date}') - JULIANDAY(engage_date) > 90
        """)
        
        avg_deal = query_database("SELECT AVG(close_value) FROM sales_pipeline WHERE deal_stage = 'Won'")
        avg_stuck = size_check[0][0] if size_check and size_check[0][0] else 0
        avg_won = avg_deal[0][0] if avg_deal and avg_deal[0][0] else 0
        
        if stuck_analysis and len(stuck_analysis) > 0:
            findings = ["Reps with most stuck deals:"]
            for agent, count, days, value in stuck_analysis:
                findings.append(f"{agent}: {int(count)} deals stuck (avg {int(days)} days, ${int(value or 0):,} at risk)")
            
            if avg_stuck > 0 and avg_won > 0 and avg_stuck > avg_won * 1.3:
                findings.append("Root cause: Deal complexity - stuck deals are {:.0f}% larger than typical deals".format((avg_stuck/avg_won - 1) * 100))
                findings.append("Likely issue: Complex enterprise deals need better qualification or executive sponsorship")
            else:
                findings.append("Root cause: Pipeline hygiene - stuck deals are normal size, indicating lack of follow-up")
                findings.append("Likely issue: Reps not actively managing pipeline or deals should be marked lost")
        else:
            findings = ["No stuck deals found in analysis period"]
        
        return {
            'root_cause': f'{sum(r[1] for r in stuck_analysis) if stuck_analysis else 0} deals stuck >90 days',
            'findings': findings,
            'recommendation_prompt': 'High churn risk from stale pipeline - implement pipeline hygiene process'
        }
    
    elif alert_type == 'territory_imbalance':
        # Analyze if imbalance is justified (top performers) or capacity issue
        load_analysis = query_database("""
            WITH rep_metrics AS (
                SELECT 
                    sales_agent,
                    COUNT(CASE WHEN deal_stage = 'Engaging' THEN 1 END) as active_deals,
                    SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END) as total_revenue,
                    SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 END), 0) as win_rate
                FROM sales_pipeline
                GROUP BY sales_agent
            )
            SELECT 
                sales_agent,
                active_deals,
                total_revenue,
                win_rate
            FROM rep_metrics
            WHERE active_deals > 0
            ORDER BY active_deals DESC
        """)
        
        findings = ["Territory load by rep:"]
        high_load_high_performance = 0
        high_load_low_performance = 0
        
        avg_revenue = sum(r[2] for r in load_analysis) / len(load_analysis) if load_analysis else 0
        
        for agent, deals, revenue, win_rate in load_analysis[:8]:
            findings.append(f"{agent}: {int(deals)} active deals (${int(revenue):,} total revenue, {win_rate:.1f}% win rate)")
            
            if deals > 50:  # High load threshold
                if revenue > avg_revenue:
                    high_load_high_performance += 1
                else:
                    high_load_low_performance += 1
        
        findings.append("")
        if high_load_low_performance > high_load_high_performance:
            findings.append("Root cause: Capacity overwhelm - high-load reps are underperforming")
            findings.append("Action needed: Redistribute territories immediately")
        else:
            findings.append("Root cause: Cherry-picking - top performers hoarding opportunities")
            findings.append("Action needed: Implement fair lead distribution rules")
        
        return {
            'root_cause': 'Uneven territory distribution creating performance issues',
            'findings': findings,
            'recommendation_prompt': 'Territory imbalance - rebalance load based on capacity and performance'
        }
    
    elif alert_type == 'slow_ramp':
        # Analyze what's different about slow ramp reps
        ramp_comparison = query_database("""
            WITH rep_first_deal AS (
                SELECT 
                    sales_agent,
                    MIN(engage_date) as first_engage,
                    MIN(CASE WHEN deal_stage = 'Won' THEN close_date END) as first_win,
                    COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) as total_wins,
                    AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) as avg_deal_size
                FROM sales_pipeline
                GROUP BY sales_agent
            )
            SELECT 
                sales_agent,
                JULIANDAY(first_win) - JULIANDAY(first_engage) as days_to_first_win,
                total_wins,
                avg_deal_size
            FROM rep_first_deal
            WHERE first_win IS NOT NULL
            AND JULIANDAY('now') - JULIANDAY(first_engage) < 180
            ORDER BY days_to_first_win DESC
        """)
        
        if ramp_comparison:
            avg_ramp = sum(r[1] for r in ramp_comparison) / len(ramp_comparison)
            
            findings = [f"Average time to first win: {int(avg_ramp)} days", "", "Slow ramp reps:"]
            
            for agent, days, wins, avg_size in ramp_comparison[:5]:
                if days > avg_ramp * 1.3:
                    findings.append(f"{agent}: {int(days)} days to first win ({int(wins)} total wins, ${int(avg_size or 0):,} avg deal)")
            
            findings.append("")
            findings.append("Root cause: Onboarding gap - new reps lack product knowledge or sales process training")
            findings.append("Pattern: Longer ramp = lower win rates in first 90 days")
        else:
            findings = ["Insufficient data for ramp analysis"]
        
        return {
            'root_cause': 'New rep productivity delayed by poor onboarding',
            'findings': findings,
            'recommendation_prompt': 'Slow new rep ramp - improve onboarding, buddy system, and early coaching'
        }
    
    elif alert_type == 'discount_frequency':
        # Analyze WHO is discounting and WHY
        discount_analysis = query_database("""
            WITH avg_deal AS (
                SELECT AVG(close_value) as avg_val FROM sales_pipeline WHERE deal_stage = 'Won'
            )
            SELECT 
                sales_agent,
                COUNT(*) as total_deals,
                COUNT(CASE WHEN close_value < (SELECT avg_val FROM avg_deal) * 0.8 THEN 1 END) as discounted_deals,
                AVG(close_value) as avg_deal_size,
                (SELECT avg_val FROM avg_deal) as team_avg
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            AND close_date >= date('now', '-90 days')
            GROUP BY sales_agent
            HAVING discounted_deals > 0
            ORDER BY discounted_deals DESC
            LIMIT 8
        """)
        
        findings = ["Reps with highest discount rates (last 90 days):"]
        
        for agent, total, discounted, avg_size, team_avg in discount_analysis:
            discount_rate = (discounted / total * 100)
            findings.append(f"{agent}: {discount_rate:.0f}% discounted ({int(discounted)}/{int(total)} deals, ${int(avg_size):,} avg)")
        
        findings.append("")
        
        # Determine if it's competitive pressure or weak negotiation
        total_discounted = sum(r[2] for r in discount_analysis)
        total_deals = sum(r[1] for r in discount_analysis)
        overall_rate = (total_discounted / total_deals * 100) if total_deals > 0 else 0
        
        if overall_rate > 40:
            findings.append("Root cause: Market competitive pressure - widespread discounting across team")
            findings.append("Action needed: Review pricing strategy and competitive positioning")
        else:
            findings.append("Root cause: Weak negotiation skills - discounting concentrated in specific reps")
            findings.append("Action needed: Sales training on value selling and objection handling")
        
        return {
            'root_cause': f'{overall_rate:.0f}% of deals heavily discounted - margin erosion',
            'findings': findings,
            'recommendation_prompt': 'High discount frequency - address pricing/negotiation issues'
        }
    
    elif alert_type == 'seasonal_drop':
        # Compare current quarter to historical quarters
        seasonal_comparison = query_database("""
            SELECT 
                CASE 
                    WHEN strftime('%m', close_date) IN ('01','02','03') THEN 'Q1'
                    WHEN strftime('%m', close_date) IN ('04','05','06') THEN 'Q2'
                    WHEN strftime('%m', close_date) IN ('07','08','09') THEN 'Q3'
                    ELSE 'Q4'
                END as quarter,
                strftime('%Y', close_date) as year,
                SUM(close_value) as revenue,
                COUNT(*) as deals,
                AVG(close_value) as avg_deal_size
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY quarter, year
            ORDER BY year DESC, quarter DESC
        """)
        
        findings = ["Quarterly revenue trend:"]
        for quarter, year, revenue, deals, avg_size in seasonal_comparison[:4]:
            findings.append(f"{year} {quarter}: ${int(revenue):,} ({int(deals)} deals, ${int(avg_size):,} avg)")
        
        findings.append("")
        
        # Determine if drop is from deal volume or deal size
        if len(seasonal_comparison) >= 2:
            current = seasonal_comparison[0]
            previous = seasonal_comparison[1]
            
            deal_volume_change = ((current[3] - previous[3]) / previous[3] * 100)
            deal_size_change = ((current[4] - previous[4]) / previous[4] * 100)
            
            if abs(deal_volume_change) > abs(deal_size_change):
                findings.append(f"Root cause: Deal volume dropped {abs(deal_volume_change):.0f}% - pipeline generation issue")
                findings.append("Action needed: Increase marketing spend and SDR activity")
            else:
                findings.append(f"Root cause: Deal size dropped {abs(deal_size_change):.0f}% - customer budget constraints")
                findings.append("Action needed: Focus on value justification and ROI messaging")
        
        return {
            'root_cause': 'Seasonal revenue decline - immediate intervention needed',
            'findings': findings,
            'recommendation_prompt': 'Seasonal performance drop - aggressive recovery plan needed'
        }
    
    return None

def get_ai_recommendations(investigation_result, alert_type):
    """Get AI-powered recommendations based on investigation"""
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return "❌ API key not configured"
        
        client = anthropic.Anthropic(api_key=api_key)
        
        prompt = f"""You are a Revenue Operations strategist. 

Problem identified: {investigation_result['root_cause']}

Key findings:
{chr(10).join(investigation_result['findings'])}

Provide a concrete action plan with:
1. **Immediate Actions** (this week)
2. **Strategic Initiatives** (this month)
3. **Metrics to Monitor**

Be specific and actionable. Keep it under 300 words."""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return message.content[0].text
        
    except Exception as e:
        return f"❌ Error generating recommendations: {str(e)}"

def get_demo_response(prompt):
    """Demo mode responses with real database queries"""
    prompt_lower = prompt.lower()
    
    # Deal count queries
    if "how many deal" in prompt_lower or "total deal" in prompt_lower or "number of deal" in prompt_lower:
        result = query_database("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN deal_stage = 'Lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN deal_stage = 'Engaging' THEN 1 ELSE 0 END) as open
            FROM sales_pipeline
        """)
        if result:
            total, won, lost, open_deals = result[0]
            return f"**Total deals:** {int(total):,} ({int(won):,} won, {int(lost):,} lost, {int(open_deals):,} open)"
    
    # Close rate / win rate queries
    if "close rate" in prompt_lower or "win rate" in prompt_lower:
        result = query_database("""
            SELECT 
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) as won,
                COUNT(*) as total
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
        """)
        if result:
            win_rate, won, total = result[0]
            return f"Our win rate is **{win_rate:.1f}%** ({int(won):,} won out of {int(total):,} closed deals)."
    
    # Lost deals queries
    if "lost deal" in prompt_lower or "losses" in prompt_lower:
        result = query_database("""
            SELECT COUNT(*), AVG(close_value)
            FROM sales_pipeline
            WHERE deal_stage = 'Lost'
        """)
        if result:
            lost_count, avg_lost_value = result[0]
            return f"We have **{int(lost_count):,} lost deals**. These represent missed opportunities averaging ${int(avg_lost_value or 0):,} each."
    
    # Time to close / sales cycle queries
    if "time to close" in prompt_lower or "sales cycle" in prompt_lower or "how long" in prompt_lower:
        result = query_database("""
            SELECT AVG(JULIANDAY(close_date) - JULIANDAY(engage_date)) as avg_days,
                   MIN(JULIANDAY(close_date) - JULIANDAY(engage_date)) as min_days,
                   MAX(JULIANDAY(close_date) - JULIANDAY(engage_date)) as max_days
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL AND engage_date IS NOT NULL
        """)
        if result:
            avg_days, min_days, max_days = result[0]
            return f"Average sales cycle: **{int(avg_days)} days** (range: {int(min_days)}-{int(max_days)} days)"
    
    # Monthly revenue queries
    if "monthly" in prompt_lower and "revenue" in prompt_lower:
        result = query_database("""
            SELECT 
                strftime('%Y-%m', close_date) as month,
                SUM(close_value) as revenue,
                COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
            GROUP BY month
            ORDER BY month DESC
            LIMIT 6
        """)
        if result:
            response = "**Monthly Revenue (Last 6 Months):**\n\n"
            for month, revenue, deals in result:
                response += f"• {month}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Quarter performance queries
    if "quarter" in prompt_lower and ("performance" in prompt_lower or "revenue" in prompt_lower):
        result = query_database("""
            SELECT 
                CASE 
                    WHEN strftime('%m', close_date) IN ('01','02','03') THEN 'Q1'
                    WHEN strftime('%m', close_date) IN ('04','05','06') THEN 'Q2'
                    WHEN strftime('%m', close_date) IN ('07','08','09') THEN 'Q3'
                    ELSE 'Q4'
                END as quarter,
                strftime('%Y', close_date) as year,
                SUM(close_value) as revenue,
                COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
            GROUP BY quarter, year
            ORDER BY year DESC, quarter DESC
        """)
        if result:
            response = "**Quarterly Performance:**\n\n"
            for quarter, year, revenue, deals in result[:4]:
                response += f"• {year} {quarter}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Best/worst product queries
    if ("best" in prompt_lower or "top" in prompt_lower) and "product" in prompt_lower:
        result = query_database("""
            SELECT product, SUM(close_value) as revenue, COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY product
            ORDER BY revenue DESC
            LIMIT 1
        """)
        if result:
            product, revenue, deals = result[0]
            return f"**Best product:** {product} with ${int(revenue):,} revenue from {int(deals)} deals"
    
    # Pipeline queries
    if "pipeline" in prompt_lower or "open" in prompt_lower:
        result = query_database("""
            SELECT COUNT(*), AVG(close_value)
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
        """)
        if result:
            count, avg_value = result[0]
            avg_value = avg_value or 2361
            estimated_value = count * avg_value
            return f"We have **{int(count):,} open deals** in the pipeline with an estimated value of **${int(estimated_value):,}** (based on ${int(avg_value):,} avg deal size)."
    
    # Revenue queries
    if "revenue" in prompt_lower and "projection" not in prompt_lower and "forecast" not in prompt_lower:
        result = query_database("""
            SELECT SUM(close_value), COUNT(*)
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
        """)
        if result:
            revenue, deals = result[0]
            return f"Total revenue is **${int(revenue):,}** from **{int(deals):,} won deals**."
    
    # Projection/Forecast queries
    if "projection" in prompt_lower or "forecast" in prompt_lower:
        result = query_database("""
            SELECT 
                SUM(close_value) as total_revenue,
                COUNT(*) as won_deals,
                JULIANDAY(MAX(close_date)) - JULIANDAY(MIN(close_date)) as days_span
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
        """)
        if result:
            revenue, deals, days = result[0]
            monthly_avg = (revenue / (days / 30)) if days > 0 else 0
            eoq_projection = monthly_avg * 3
            eoy_projection = monthly_avg * 12
            
            return f"""**Revenue Projections (Based on Historical Performance):**

📊 **End of Quarter (EOQ):** ${int(eoq_projection):,}
🎯 **End of Year (EOY):** ${int(eoy_projection):,}

*Based on:*
- Historical revenue: ${int(revenue):,}
- Time period: {int(days)} days
- Monthly average: ${int(monthly_avg):,}

⚠️ *Note: Projections assume current performance trends continue.*"""
    
    # Top reps queries
    if "top" in prompt_lower and ("rep" in prompt_lower or "agent" in prompt_lower or "sales" in prompt_lower):
        result = query_database("""
            SELECT sales_agent, SUM(close_value) as revenue, COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
            ORDER BY revenue DESC
            LIMIT 5
        """)
        if result:
            response = "**Top 5 Sales Reps by Revenue:**\n\n"
            for i, (agent, revenue, deals) in enumerate(result, 1):
                response += f"{i}. {agent}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Bottom performers queries
    if "bottom" in prompt_lower and ("rep" in prompt_lower or "agent" in prompt_lower or "performer" in prompt_lower):
        result = query_database("""
            SELECT sales_agent, SUM(close_value) as revenue, COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
            ORDER BY revenue ASC
            LIMIT 5
        """)
        if result:
            response = "**Bottom 5 Sales Reps by Revenue:**\n\n"
            for i, (agent, revenue, deals) in enumerate(result, 1):
                response += f"{i}. {agent}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Sales agents / team overview
    if ("sales agent" in prompt_lower or "how many" in prompt_lower) and ("rep" in prompt_lower or "agent" in prompt_lower):
        result = query_database("""
            SELECT 
                COUNT(DISTINCT sales_agent) as total_reps,
                AVG(rep_revenue) as avg_revenue,
                MAX(rep_revenue) as top_revenue,
                MIN(rep_revenue) as bottom_revenue
            FROM (
                SELECT sales_agent, SUM(close_value) as rep_revenue
                FROM sales_pipeline
                WHERE deal_stage = 'Won'
                GROUP BY sales_agent
            )
        """)
        if result:
            total, avg_rev, top_rev, bottom_rev = result[0]
            return f"""**Sales Team Overview:**

Total reps: **{int(total)}**
Average revenue per rep: **${int(avg_rev):,}**
Top performer: **${int(top_rev):,}**
Bottom performer: **${int(bottom_rev):,}**

*Range: ${int(bottom_rev):,} - ${int(top_rev):,}*"""
    
    # Product performance queries
    if "product" in prompt_lower:
        result = query_database("""
            SELECT product, COUNT(*) as deals, SUM(close_value) as revenue
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY product
            ORDER BY revenue DESC
        """)
        if result:
            response = "**Product Performance:**\n\n"
            for product, deals, revenue in result:
                response += f"• {product}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Deal size queries
    if "deal size" in prompt_lower or "average deal" in prompt_lower:
        result = query_database("""
            SELECT AVG(close_value), MIN(close_value), MAX(close_value)
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
        """)
        if result:
            avg, min_val, max_val = result[0]
            return f"Average deal size: **${int(avg):,}** (range: ${int(min_val):,} - ${int(max_val):,})"
    
    # CAC (Customer Acquisition Cost) - proxy calculation
    if "cac" in prompt_lower or "acquisition cost" in prompt_lower or "customer acquisition" in prompt_lower:
        result = query_database("""
            SELECT 
                COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) as won_deals,
                COUNT(*) as total_attempts
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
        """)
        if result:
            won, total = result[0]
            estimated_cost_per_attempt = 500
            total_sales_cost = total * estimated_cost_per_attempt
            cac = total_sales_cost / won if won > 0 else 0
            
            return f"""**Customer Acquisition Cost (Estimated):**

CAC: **${int(cac):,}** per customer

*Calculation:*
- Total deal attempts: {int(total):,} (won + lost)
- Won deals: {int(won):,}
- Estimated cost per attempt: ${estimated_cost_per_attempt:,}
- Total sales cost: ${int(total_sales_cost):,}

⚠️ *Note: This is a proxy CAC based on estimated sales effort. Actual CAC requires marketing/sales spend data.*"""
    
    # Velocity (Pipeline velocity)
    if "velocity" in prompt_lower or "pipeline velocity" in prompt_lower:
        result = query_database("""
            WITH metrics AS (
                SELECT 
                    COUNT(CASE WHEN deal_stage = 'Engaging' THEN 1 END) as pipeline_deals,
                    AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) as avg_deal_value,
                    SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 END), 0) as win_rate,
                    AVG(CASE WHEN deal_stage = 'Won' THEN JULIANDAY(close_date) - JULIANDAY(engage_date) END) as avg_cycle_days
                FROM sales_pipeline
            )
            SELECT 
                pipeline_deals,
                avg_deal_value,
                win_rate,
                avg_cycle_days,
                (pipeline_deals * avg_deal_value * (win_rate/100.0)) / avg_cycle_days as daily_velocity,
                (pipeline_deals * avg_deal_value * (win_rate/100.0)) / avg_cycle_days * 30 as monthly_velocity
            FROM metrics
        """)
        if result:
            pipeline, avg_val, win_rate, cycle, daily_vel, monthly_vel = result[0]
            
            return f"""**Pipeline Velocity:**

Monthly velocity: **${int(monthly_vel):,}**
Daily velocity: **${int(daily_vel):,}**

*Formula: (Pipeline deals × Avg deal value × Win rate) / Avg sales cycle*

**Breakdown:**
- Pipeline deals: {int(pipeline):,}
- Avg deal value: ${int(avg_val):,}
- Win rate: {win_rate:.1f}%
- Avg sales cycle: {int(cycle)} days

This represents the amount of revenue flowing through your pipeline per month."""
    
    # Default response
    return "I can help with questions about win rate, pipeline, revenue, projections, sales cycle, top reps, products, deal sizes, CAC, velocity, monthly/quarterly performance, and more. Try asking: *'What's our projection?'* or *'Show me monthly revenue'*"
    if "win rate" in prompt_lower:
        result = query_database("""
            SELECT 
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) as won,
                COUNT(*) as total
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
        """)
        if result:
            win_rate, won, total = result[0]
            return f"Our win rate is **{win_rate:.1f}%** ({int(won):,} won out of {int(total):,} closed deals)."
    
    # Pipeline queries
    if "pipeline" in prompt_lower or "open" in prompt_lower:
        result = query_database("""
            SELECT COUNT(*), AVG(close_value)
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
        """)
        if result:
            count, avg_value = result[0]
            avg_value = avg_value or 2361
            estimated_value = count * avg_value
            return f"We have **{int(count):,} open deals** in the pipeline with an estimated value of **${int(estimated_value):,}** (based on ${int(avg_value):,} avg deal size)."
    
    # Revenue queries
    if "revenue" in prompt_lower and "projection" not in prompt_lower and "forecast" not in prompt_lower:
        result = query_database("""
            SELECT SUM(close_value), COUNT(*)
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
        """)
        if result:
            revenue, deals = result[0]
            return f"Total revenue is **${int(revenue):,}** from **{int(deals):,} won deals**."
    
    # Projection/Forecast queries
    if "projection" in prompt_lower or "forecast" in prompt_lower:
        result = query_database("""
            SELECT 
                SUM(close_value) as total_revenue,
                COUNT(*) as won_deals,
                JULIANDAY(MAX(close_date)) - JULIANDAY(MIN(close_date)) as days_span
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL
        """)
        if result:
            revenue, deals, days = result[0]
            monthly_avg = (revenue / (days / 30)) if days > 0 else 0
            eoq_projection = monthly_avg * 3
            eoy_projection = monthly_avg * 12
            
            return f"""**Revenue Projections (Based on Historical Performance):**

📊 **End of Quarter (EOQ):** ${int(eoq_projection):,}
🎯 **End of Year (EOY):** ${int(eoy_projection):,}

*Based on:*
- Historical revenue: ${int(revenue):,}
- Time period: {int(days)} days
- Monthly average: ${int(monthly_avg):,}

⚠️ *Note: Projections assume current performance trends continue.*"""
    
    # Sales cycle queries
    if "sales cycle" in prompt_lower or "how long" in prompt_lower:
        result = query_database("""
            SELECT AVG(JULIANDAY(close_date) - JULIANDAY(engage_date)) as avg_days
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_date IS NOT NULL AND engage_date IS NOT NULL
        """)
        if result:
            avg_days = result[0][0]
            return f"Our average sales cycle is **{int(avg_days)} days** from engage to close."
    
    # Top reps queries
    if "top" in prompt_lower and ("rep" in prompt_lower or "agent" in prompt_lower or "sales" in prompt_lower):
        result = query_database("""
            SELECT sales_agent, SUM(close_value) as revenue, COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
            ORDER BY revenue DESC
            LIMIT 5
        """)
        if result:
            response = "**Top 5 Sales Reps by Revenue:**\n\n"
            for i, (agent, revenue, deals) in enumerate(result, 1):
                response += f"{i}. {agent}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Bottom performers queries
    if "bottom" in prompt_lower and ("rep" in prompt_lower or "agent" in prompt_lower or "performer" in prompt_lower):
        result = query_database("""
            SELECT sales_agent, SUM(close_value) as revenue, COUNT(*) as deals
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
            ORDER BY revenue ASC
            LIMIT 5
        """)
        if result:
            response = "**Bottom 5 Sales Reps by Revenue:**\n\n"
            for i, (agent, revenue, deals) in enumerate(result, 1):
                response += f"{i}. {agent}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Sales agents / team overview
    if ("sales agent" in prompt_lower or "how many" in prompt_lower) and ("rep" in prompt_lower or "agent" in prompt_lower):
        result = query_database("""
            SELECT 
                COUNT(DISTINCT sales_agent) as total_reps,
                AVG(rep_revenue) as avg_revenue,
                MAX(rep_revenue) as top_revenue,
                MIN(rep_revenue) as bottom_revenue
            FROM (
                SELECT sales_agent, SUM(close_value) as rep_revenue
                FROM sales_pipeline
                WHERE deal_stage = 'Won'
                GROUP BY sales_agent
            )
        """)
        if result:
            total, avg_rev, top_rev, bottom_rev = result[0]
            return f"""**Sales Team Overview:**

Total reps: **{int(total)}**
Average revenue per rep: **${int(avg_rev):,}**
Top performer: **${int(top_rev):,}**
Bottom performer: **${int(bottom_rev):,}**

*Range: ${int(bottom_rev):,} - ${int(top_rev):,}*"""
    
    # Product performance queries
    if "product" in prompt_lower:
        result = query_database("""
            SELECT product, COUNT(*) as deals, SUM(close_value) as revenue
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY product
            ORDER BY revenue DESC
        """)
        if result:
            response = "**Product Performance:**\n\n"
            for product, deals, revenue in result:
                response += f"• {product}: ${int(revenue):,} ({int(deals)} deals)\n"
            return response
    
    # Deal size queries
    if "deal size" in prompt_lower or "average deal" in prompt_lower:
        result = query_database("""
            SELECT AVG(close_value), MIN(close_value), MAX(close_value)
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
        """)
        if result:
            avg, min_val, max_val = result[0]
            return f"Average deal size: **${int(avg):,}** (range: ${int(min_val):,} - ${int(max_val):,})"
    
    # CAC (Customer Acquisition Cost) - proxy calculation
    if "cac" in prompt_lower or "acquisition cost" in prompt_lower or "customer acquisition" in prompt_lower:
        result = query_database("""
            SELECT 
                COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) as won_deals,
                COUNT(*) as total_attempts
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
        """)
        if result:
            won, total = result[0]
            # Proxy: Assume $500 cost per sales attempt (calls, demos, travel)
            estimated_cost_per_attempt = 500
            total_sales_cost = total * estimated_cost_per_attempt
            cac = total_sales_cost / won if won > 0 else 0
            
            return f"""**Customer Acquisition Cost (Estimated):**

CAC: **${int(cac):,}** per customer

*Calculation:*
- Total deal attempts: {int(total):,} (won + lost)
- Won deals: {int(won):,}
- Estimated cost per attempt: ${estimated_cost_per_attempt:,}
- Total sales cost: ${int(total_sales_cost):,}

⚠️ *Note: This is a proxy CAC based on estimated sales effort. Actual CAC requires marketing/sales spend data.*"""
    
    # Velocity (Pipeline velocity)
    if "velocity" in prompt_lower or "pipeline velocity" in prompt_lower:
        result = query_database("""
            WITH metrics AS (
                SELECT 
                    COUNT(CASE WHEN deal_stage = 'Engaging' THEN 1 END) as pipeline_deals,
                    AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) as avg_deal_value,
                    SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN 1 END), 0) as win_rate,
                    AVG(CASE WHEN deal_stage = 'Won' THEN JULIANDAY(close_date) - JULIANDAY(engage_date) END) as avg_cycle_days
                FROM sales_pipeline
            )
            SELECT 
                pipeline_deals,
                avg_deal_value,
                win_rate,
                avg_cycle_days,
                (pipeline_deals * avg_deal_value * (win_rate/100.0)) / avg_cycle_days as daily_velocity,
                (pipeline_deals * avg_deal_value * (win_rate/100.0)) / avg_cycle_days * 30 as monthly_velocity
            FROM metrics
        """)
        if result:
            pipeline, avg_val, win_rate, cycle, daily_vel, monthly_vel = result[0]
            
            return f"""**Pipeline Velocity:**

Monthly velocity: **${int(monthly_vel):,}**
Daily velocity: **${int(daily_vel):,}**

*Formula: (Pipeline deals × Avg deal value × Win rate) / Avg sales cycle*

**Breakdown:**
- Pipeline deals: {int(pipeline):,}
- Avg deal value: ${int(avg_val):,}
- Win rate: {win_rate:.1f}%
- Avg sales cycle: {int(cycle)} days

This represents the amount of revenue flowing through your pipeline per month."""
    
    # Default response
    return "I can help with questions about win rate, pipeline, revenue, projections, sales cycle, top reps, products, deal sizes, CAC, and velocity. Try asking: *'What's our projection?'* or *'Show me pipeline velocity'*"

def get_claude_response(prompt):
    """Live mode with database access"""
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return "❌ API key not found."
        
        schema_info = """
        Database: sales_pipeline table
        Columns: opportunity_id, sales_agent, product, account, deal_stage (Won/Lost/Engaging), 
        engage_date, close_date, close_value
        SQLite syntax. Use JULIANDAY() for date calculations.
        """
        
        client = anthropic.Anthropic(api_key=api_key)
        
        system_prompt = f"""You are a Revenue Operations analyst.

{schema_info}

Write SQL in <sql></sql> tags. Provide executive insights."""
        
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text
        
        import re
        sql_match = re.search(r'<sql>(.*?)</sql>', response_text, re.DOTALL)
        
        if sql_match:
            sql_query = sql_match.group(1).strip()
            results = query_database(sql_query)
            
            if results:
                results_text = f"Query returned {len(results)} rows:\n"
                for row in results[:50]:
                    results_text += f"{row}\n"
                
                analysis_message = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1000,
                    system="Revenue Operations analyst. Provide executive insights.",
                    messages=[{"role": "user", "content": f"Question: {prompt}\n\nData:\n{results_text}\n\nProvide business insights."}]
                )
                
                return analysis_message.content[0].text
            else:
                return "❌ No data found."
        else:
            return response_text
        
    except Exception as e:
        return f"❌ Error: {str(e)}"

# ============================================
# MAIN UI
# ============================================

# Header
st.title("📊 Revenue Intelligence Platform")
st.caption("Executive dashboard with AI-powered diagnostics")
st.markdown("**Built by Carlos Gonzalez** | [LinkedIn](https://linkedin.com/in/carlosgonzalez01)")

st.divider()

# Executive Metrics Dashboard
st.subheader("Executive Metrics (2017 Historical Data)")

metrics = get_executive_metrics()

# Top row: Core performance metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "💰 Total Revenue (2017)", 
        f"${metrics.get('qtd_revenue', 0):,}"
    )

with col2:
    st.metric(
        "🎯 Won Deals",
        f"{metrics.get('won_deals', 0):,}"
    )

with col3:
    st.metric(
        "📈 Win Rate",
        f"{metrics.get('win_rate', 0)}%"
    )

with col4:
    st.metric(
        "💵 Avg Deal Size",
        f"${metrics.get('avg_deal_size', 0):,}"
    )

# Bottom row: Forward-looking metrics
col5, col6, col7, col8 = st.columns(4)

with col5:
    health_score = metrics.get('pipeline_health', 0)
    health_color = "🟢" if health_score >= 3 else "🟡" if health_score >= 2 else "🔴"
    st.metric(
        f"{health_color} Pipeline Health",
        f"{health_score}x",
        f"${metrics.get('pipeline_value', 0):,}"
    )

with col6:
    st.metric(
        "📊 EOQ Projection",
        f"${metrics.get('eoq_projection', 0):,}"
    )

with col7:
    st.metric(
        "🎯 EOY Projection",
        f"${metrics.get('quarter_projection', 0):,}"
    )

with col8:
    st.metric(
        "🏆 Top Product",
        metrics.get('top_region', 'N/A'),
        f"${metrics.get('top_region_revenue', 0):,}"
    )

st.divider()

# Smart Alerts Section
st.subheader("⚠️ Alerts & Diagnostics")

alerts = detect_alerts()

# Show collapsed button with alert count
if alerts:
    alert_count = len(alerts)
    critical_count = sum(1 for a in alerts if a['severity'] == 'critical')
    
    # Use expander to collapse alerts by default
    with st.expander(f"🔍 Check Alerts ({alert_count} {'🚨' if critical_count > 0 else '⚠️'} - {critical_count} critical)", expanded=False):
        for idx, alert in enumerate(alerts):
            alert_class = "alert-critical" if alert['severity'] == 'critical' else "alert-warning"
            
            with st.container():
                st.markdown(f"""
                <div class="{alert_class}" style="padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid {'#ff4444' if alert['severity'] == 'critical' else '#ffaa00'};">
                    <strong>{'🚨' if alert['severity'] == 'critical' else '⚠️'} {alert['title']}</strong><br>
                    {alert['message']}</div>
                """, unsafe_allow_html=True)
            
            # Step 1: Show Investigate button
            if idx not in st.session_state.show_investigation:
                if st.button(f"🔍 Investigate", key=f"investigate_{idx}"):
                    alert_type = alert.get('type', 'unknown')
                    if alert_type != 'unknown':
                        st.session_state.show_investigation[idx] = investigate_alert(alert_type)
                        st.rerun()
                    else:
                        st.error("Alert type not found - please refresh the page")
            
            # Step 2: Show investigation results + Get Recommendations button
            if idx in st.session_state.show_investigation:
                investigation = st.session_state.show_investigation[idx]
                
                st.info(f"**Root Cause:** {investigation['root_cause']}")
                for finding in investigation['findings']:
                    st.write(f"• {finding}")
                
                # Step 3: Get Recommendations (auto-enables Live Mode, counts toward 5 questions)
                remaining_questions = 5 - st.session_state.live_mode_questions
                
                if remaining_questions > 0:
                    with st.expander(f"💡 Get AI Recommendations (Password Required - {remaining_questions}/5 AI uses remaining)", expanded=True):
                        password = st.text_input("Enter password:", type="password", key=f"pwd_{idx}")
                        
                        # Check if we already have recommendations for this alert
                        rec_key = f"recommendations_{idx}"
                        
                        if st.button("Get Recommendations", key=f"recommend_{idx}"):
                            if password == os.getenv("LIVE_MODE_PASSWORD", "recruiter2025"):
                                # Auto-enable Live Mode and count this as a question
                                st.session_state.demo_mode = False
                                st.session_state.live_mode_questions += 1
                                
                                with st.spinner("Generating AI recommendations..."):
                                    recommendations = get_ai_recommendations(investigation, alert['type'])
                                    
                                    # Add to chat history so user can see and continue conversation
                                    st.session_state.messages.append({
                                        "role": "user",
                                        "content": f"Based on the {alert['title']} investigation, provide strategic recommendations."
                                    })
                                    st.session_state.messages.append({
                                        "role": "assistant",
                                        "content": recommendations
                                    })
                                    
                                    # Set flag to show new message indicator
                                    st.session_state.new_ai_message = True
                                
                                st.success(f"✅ Recommendations generated!")
                                st.info("📍 **Scroll down to see AI recommendations in chat below** ⬇️")
                                
                                if st.session_state.live_mode_questions >= 5:
                                    st.warning("🚫 **AI limit reached (5/5).** Live Mode disabled. Continue using Demo Mode for unlimited database queries.")
                                
                                st.rerun()
                            elif password:
                                st.error("❌ Incorrect password")
                        
                        # Don't display recommendations in expander anymore - only in chat
                else:
                    st.warning("🚫 **AI limit reached (5/5).** Live Mode disabled. Continue using Demo Mode for unlimited database queries.")
else:
    st.success("✅ No critical alerts - business metrics healthy")

st.divider()

# Chatbot Section
st.subheader("💬 Ask Additional Questions")

# Show new message indicator if AI just responded
if st.session_state.get('new_ai_message', False):
    st.success("🆕 **New AI recommendations below!** You can continue the conversation or ask follow-up questions.")
    st.session_state.new_ai_message = False

# Welcome intro (only show if no messages)
if len(st.session_state.messages) == 0:
    st.markdown("""
    **👋 I'm Carlos, your Revenue Operations analyst.** I built this AI-powered tool to help organizations get instant insights from their sales data.
    
    **Try asking me:**
    - *"What's driving our win rate performance?"*
    - *"Which sales reps need coaching vs recognition?"*
    - *"How healthy is our pipeline for next quarter?"*
    - *"What's our customer acquisition cost trend?"*
    
    **Two ways to get insights:**
    - **Demo Mode (Free):** Pre-built analytics querying real database - unlimited questions
    - **Live Mode (AI):** Claude AI writes custom SQL on the fly for complex ad-hoc analysis - 5 questions max
    
    **Want to see it work?** Use the Admin Panel on the left to simulate events (close deals, add opportunities), then ask me the same question again to see real-time updates!
    """)
    st.divider()

# Clear chat button (top right, only shows if messages exist)
if len(st.session_state.messages) > 0:
    col_clear1, col_clear2 = st.columns([5, 1])
    with col_clear2:
        if st.button("🗑️ Clear"):
            st.session_state.messages = []
            st.rerun()

# Chat messages
for message in st.session_state.messages:
    if message["role"] == "assistant":
        with st.chat_message("assistant", avatar="avatar.png"):
            st.markdown(message["content"])
    else:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# Live Mode banner above chat input
if not st.session_state.demo_mode and st.session_state.live_mode_questions < 5:
    remaining = 5 - st.session_state.live_mode_questions
    st.info(f"💸 **Live Mode: {remaining}/5 questions remaining**")
elif st.session_state.live_mode_questions >= 5:
    st.warning("🚫 **AI limit reached (5/5).** Demo Mode available for unlimited database queries.")

# Chat input
if prompt := st.chat_input("Ask about revenue, pipeline, or team performance..."):
    if not st.session_state.demo_mode and st.session_state.live_mode_questions >= 5:
        st.error("🚫 AI limit reached. Using Demo Mode.")
        st.session_state.demo_mode = True
    
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant", avatar="avatar.png"):
        with st.spinner("Analyzing..."):
            if st.session_state.demo_mode:
                response = get_demo_response(prompt)
            else:
                response = get_claude_response(prompt)
                st.session_state.live_mode_questions += 1
        
        st.markdown(response)
    
    st.session_state.messages.append({"role": "assistant", "content": response})

# Sidebar
with st.sidebar:
    st.header("💼 Carlos Gonzalez")
    st.caption("Revenue Operations Analyst")
    
    try:
        st.image("avatar.png", width=150)
    except:
        pass
    
    st.write("**Portfolio Project**")
    st.write("AI-powered revenue diagnostics with automated root cause analysis")
    
    st.info("👋 **Looking to hire?** [Connect on LinkedIn](https://linkedin.com/in/carlosgonzalez01)")
    
    st.divider()
    
    st.header("About This Tool")
    st.write("Analyzes 8,800+ real B2B deals from [Kaggle CRM dataset](https://www.kaggle.com/datasets/innocentmfa/crm-sales-opportunities)")
    
    st.divider()
    
    # How This Works button
    if st.button("ℹ️ How This Platform Works", use_container_width=True):
        st.session_state.messages = []  # Clear chat
        st.session_state.messages.append({
            "role": "assistant",
            "content": """## 📊 Revenue Intelligence Platform - How It Works

**I'm an AI-powered diagnostic engine that automates what Revenue Operations analysts do manually.**

---

### 🚨 **Step 1: Automated Monitoring**

I continuously monitor **10 critical business metrics**:

1. **Win Rate Trends** - Detecting 3%+ declines quarter-over-quarter
2. **Pipeline Health** - Flagging coverage below 4x monthly quota
3. **Sales Velocity** - Catching cycle time increases of 15%+
4. **Rep Performance** - Identifying when <40% hit above-average
5. **Product Mix** - Detecting revenue concentration (top product 3x+ bigger)
6. **Churn Risk** - Finding deals stuck in pipeline >90 days
7. **Territory Balance** - Spotting 3x+ load imbalances across reps
8. **Rep Ramp Time** - Tracking new hires taking 50%+ longer to close
9. **Discount Patterns** - Alerting when >30% of deals are heavily discounted
10. **Seasonal Changes** - Detecting 25%+ revenue drops quarter-over-quarter

**Each metric runs SQL queries against your 8,800+ deal database in real-time.**

---

### 🔍 **Step 2: Root Cause Analysis**

When I detect a problem, I don't just tell you "win rate is down" - I tell you **WHY**.

**My diagnostic trees branch based on data:**

**Example - Revenue Concentration Alert:**
```
Alert Triggered: Only 11/28 reps above average
↓
Investigation Query 1: Get all rep metrics
  - Revenue, deals, win rate, deal size per rep
  - Compare each to team averages
↓
Branch Logic: What's the primary issue per rep?
  - Win rate 10%+ below average? → "Win rate issue (52.1%)"
  - Deal size $500+ below average? → "Deal size issue ($1,685)"
  - Otherwise? → "Activity issue (75 deals)"
↓
Rank by business impact ($ gap from average)
↓
Output: "Niesha Huffines: $170,341 below avg - Deal size issue ($1,685)"
```

**I identify the specific, actionable problem - not just symptoms.**

---

### 💡 **Step 3: AI-Powered Recommendations**

After diagnosing the root cause, I generate strategic action plans using Claude AI.

**The recommendations include:**
- **Immediate Actions** (this week) - Specific rep names, exact steps
- **Strategic Initiatives** (this month) - Process improvements, tool deployments
- **Metrics to Monitor** - What to track weekly/monthly
- **Success Targets** - 90-day recovery goals with $ amounts

**Example output:**
"Emergency coaching for Lajuana, Moses, Anna on value-based selling. Target: Recover $1.4M in at-risk revenue within 90 days through 25% deal size improvement."

---

### 🎯 **Why This Matters**

**Traditional approach:**
1. Manager looks at dashboard
2. Sees "revenue down"
3. Schedules meetings to investigate
4. Manually analyzes data
5. Develops plan
**Time: 2-3 weeks**

**This platform:**
1. Auto-detects issue
2. Runs diagnostic tree
3. Identifies root cause
4. Generates action plan
**Time: <30 seconds**

---

### 🛠️ **Technical Stack**

- **Database:** SQLite (8,800+ B2B deals, 2017 historical data)
- **Backend:** Python, Streamlit
- **AI:** Claude 4 Sonnet API for strategic recommendations
- **Queries:** Complex SQL with CTEs, window functions, date calculations
- **Diagnostics:** Rule-based decision trees with 1-2 SQL queries per investigation

---

### 📈 **Demo Features**

**Try it yourself:**
1. **Admin Panel** (left sidebar) - Simulate business events:
   - Close deals, mark as lost, add opportunities
   - Hire/fire sales agents
   - Watch alerts update in real-time

2. **Chatbot** (below) - Ask questions like:
   - "What's our win rate?"
   - "Show me pipeline velocity"
   - "Which reps need coaching?"

3. **Live Mode** (toggle) - Claude AI writes custom SQL for complex analysis
   - Password protected: `recruiter2025`
   - Limited to 5 AI queries

---

### 👤 **Built by Carlos Gonzalez**

I'm a B2B sales professional (8 years) transitioning into Revenue Operations through analytics.

This platform demonstrates:
- SQL proficiency (complex queries, CTEs, window functions)
- Python development (Streamlit, API integration)
- AI implementation (Claude API, prompt engineering)
- RevOps domain expertise (sales operations, pipeline management)
- Product thinking (identified real problem, built solution)

**Want to connect?**

**[Connect with me on LinkedIn →](https://linkedin.com/in/carlosgonzalez01)**

Let's discuss how I can bring this level of technical execution and strategic thinking to your Revenue Operations team.

---

*This entire platform was built in 6 days as a portfolio project. Imagine what I can build with your data and systems.*"""
        })
        st.session_state.new_ai_message = True
        st.rerun()
    
    st.divider()
    
    # Mode toggle (only show if under 5 questions)
    if st.session_state.live_mode_questions < 5:
        col_left, col_switch, col_right = st.columns([1, 0.4, 1])
        with col_left:
            st.markdown("<div style='text-align: right; padding-top: 8px;'><strong>Demo</strong></div>", unsafe_allow_html=True)
        with col_switch:
            # Toggle with password protection
            if st.session_state.demo_mode:
                # In Demo Mode - clicking toggle requires password
                if st.toggle("", value=False, label_visibility="collapsed", key="mode_toggle"):
                    # User tried to enable Live Mode - ask for password
                    password = st.text_input("Enter password to enable Live Mode:", type="password", key="live_pwd")
                    if password == os.getenv("LIVE_MODE_PASSWORD", "recruiter2025"):
                        st.session_state.demo_mode = False
                        st.success("✅ Live Mode activated!")
                        st.rerun()
                    elif password:
                        st.error("❌ Incorrect password")
            else:
                # In Live Mode - clicking toggle goes back to Demo (no password needed)
                if st.toggle("", value=True, label_visibility="collapsed", key="mode_toggle"):
                    pass  # Stay in Live Mode
                else:
                    st.session_state.demo_mode = True
                    st.rerun()
        with col_right:
            st.markdown("<div style='padding-top: 8px;'><strong>Live</strong></div>", unsafe_allow_html=True)
    
    st.divider()
    
    # Mode status
    if st.session_state.live_mode_questions >= 5:
        st.error("🚫 AI Limit Reached")
        st.caption("Demo Mode only - unlimited queries")
    elif st.session_state.demo_mode:
        st.success("💰 Demo Mode Active")
        st.caption("Unlimited queries, real database")
    else:
        remaining = 5 - st.session_state.live_mode_questions
        st.warning(f"💸 Live Mode: {remaining}/5 AI questions")
        st.caption("AI-powered analysis active")
    
    st.divider()
    
    # Admin Panel
    st.header("🎮 Admin Panel")
    st.caption("Simulate business events")
    
    # Get current avg deal size for realistic simulations
    avg_deal_query = query_database("SELECT AVG(close_value) FROM sales_pipeline WHERE deal_stage = 'Won'")
    avg_deal_size = int(avg_deal_query[0][0]) if avg_deal_query else 2360
    
    multiplier = st.selectbox("Multiplier", [10, 20, 30, 40, 50, 100])
    
    if st.button(f"💰 Close {multiplier} × ${avg_deal_size:,} Deals", use_container_width=True):
        try:
            conn = sqlite3.connect('revenue_data.db')
            cursor = conn.cursor()
            
            # Get reference date
            cursor.execute("SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL")
            reference_date = cursor.fetchone()[0] or '2017-12-31'
            
            # Realistic closure pattern: 70% newest deals, 25% median deals, 5% oldest
            newest_count = int(multiplier * 0.7)
            median_count = int(multiplier * 0.25)
            oldest_count = multiplier - newest_count - median_count
            
            # Get newest deals (30-90 days old - healthy velocity)
            cursor.execute(f"""
                SELECT opportunity_id
                FROM sales_pipeline
                WHERE deal_stage = 'Engaging'
                AND engage_date IS NOT NULL
                ORDER BY engage_date DESC
                LIMIT {newest_count}
            """)
            newest_deals = [row[0] for row in cursor.fetchall()]
            
            # Get median age deals
            cursor.execute(f"""
                WITH deal_ages AS (
                    SELECT 
                        opportunity_id,
                        ROW_NUMBER() OVER (ORDER BY engage_date DESC) as rn,
                        COUNT(*) OVER () as total_count
                    FROM sales_pipeline
                    WHERE deal_stage = 'Engaging'
                    AND engage_date IS NOT NULL
                )
                SELECT opportunity_id
                FROM deal_ages
                WHERE rn BETWEEN (total_count / 2) - {median_count//2} AND (total_count / 2) + {median_count//2}
                LIMIT {median_count}
            """)
            median_deals = [row[0] for row in cursor.fetchall()]
            
            # Get oldest deals (stuck pipeline - rarely close)
            cursor.execute(f"""
                SELECT opportunity_id
                FROM sales_pipeline
                WHERE deal_stage = 'Engaging'
                AND engage_date IS NOT NULL
                ORDER BY engage_date ASC
                LIMIT {oldest_count}
            """)
            oldest_deals = [row[0] for row in cursor.fetchall()]
            
            # Close all selected deals
            all_deals = newest_deals + median_deals + oldest_deals
            
            for opp_id in all_deals:
                cursor.execute("""
                    UPDATE sales_pipeline 
                    SET deal_stage = 'Won', close_value = ?, close_date = ?
                    WHERE opportunity_id = ?
                """, (avg_deal_size, reference_date, opp_id))
            
            conn.commit()
            st.success(f"✅ Closed {len(all_deals)} deals: {newest_count} fresh, {median_count} median-age, {oldest_count} stuck!")
            st.balloons()
            
            # Clear investigation cache so findings refresh
            st.session_state.show_investigation = {}
            
            cursor.close()
            conn.close()
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button(f"❌ Mark {multiplier} as Lost", use_container_width=True):
        try:
            conn = sqlite3.connect('revenue_data.db')
            cursor = conn.cursor()
            
            # Mark NEWEST deals as lost (different from closing old stuck deals)
            cursor.execute(f"""
                SELECT opportunity_id 
                FROM sales_pipeline 
                WHERE deal_stage = 'Engaging' 
                ORDER BY engage_date DESC
                LIMIT {multiplier}
            """)
            results = cursor.fetchall()
            
            if results:
                for (opp_id,) in results:
                    cursor.execute("""
                        UPDATE sales_pipeline 
                        SET deal_stage = 'Lost', 
                            close_date = (
                                SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL
                            )
                        WHERE opportunity_id = ?
                    """, (opp_id,))
                
                conn.commit()
                st.warning(f"Marked {len(results)} as lost")
                
                # Clear investigation cache
                st.session_state.show_investigation = {}
            
            cursor.close()
            conn.close()
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button(f"➕ Add {multiplier} × ${avg_deal_size:,} Opps", use_container_width=True):
        try:
            conn = sqlite3.connect('revenue_data.db')
            cursor = conn.cursor()
            
            # Get reference date from dataset
            cursor.execute("SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL")
            reference_date = cursor.fetchone()[0] or '2017-12-31'
            
            cursor.execute("SELECT DISTINCT sales_agent FROM sales_pipeline")
            agents = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT product FROM sales_pipeline WHERE product IS NOT NULL")
            products = [row[0] for row in cursor.fetchall()]
            
            for _ in range(multiplier):
                opp_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                agent = random.choice(agents)
                product = random.choice(products)
                
                cursor.execute("""
                    INSERT INTO sales_pipeline 
                    (opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
                    VALUES (?, ?, ?, 'New Opportunity', 'Engaging', ?, NULL, ?)
                """, (opp_id, agent, product, reference_date, avg_deal_size))
            
            conn.commit()
            st.success(f"✅ Added {multiplier} opportunities!")
            
            # Clear investigation cache
            st.session_state.show_investigation = {}
            
            cursor.close()
            conn.close()
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button("👥 Hire New Sales Agent", use_container_width=True):
        try:
            conn = sqlite3.connect('revenue_data.db')
            cursor = conn.cursor()
            
            # Get reference date from dataset
            cursor.execute("SELECT MAX(close_date) FROM sales_pipeline WHERE close_date IS NOT NULL")
            reference_date = cursor.fetchone()[0] or '2017-12-31'
            
            # Count existing admin panel agents
            cursor.execute("SELECT COUNT(*) FROM sales_pipeline WHERE sales_agent LIKE 'Admin Panel Agent%'")
            admin_count = cursor.fetchone()[0]
            new_agent = f"Admin Panel Agent #{admin_count + 1}"
            
            # Add 5 sample deals for the new agent
            cursor.execute("SELECT DISTINCT product FROM sales_pipeline WHERE product IS NOT NULL")
            products = [row[0] for row in cursor.fetchall()]
            
            for _ in range(5):
                opp_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                cursor.execute("""
                    INSERT INTO sales_pipeline 
                    (opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
                    VALUES (?, ?, ?, 'New Account', 'Engaging', ?, NULL, ?)
                """, (opp_id, new_agent, random.choice(products), reference_date, avg_deal_size))
            
            conn.commit()
            st.success(f"✅ Hired {new_agent} with 5 initial opportunities!")
            
            # Clear investigation cache
            st.session_state.show_investigation = {}
            
            cursor.close()
            conn.close()
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button("🔻 Let Go Bottom Performer", use_container_width=True):
        try:
            conn = sqlite3.connect('revenue_data.db')
            cursor = conn.cursor()
            
            # Find lowest performing agent
            cursor.execute("""
                SELECT sales_agent, SUM(close_value) as revenue
                FROM sales_pipeline
                WHERE deal_stage = 'Won'
                GROUP BY sales_agent
                ORDER BY revenue ASC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                agent_to_remove = result[0]
                
                # Delete all their deals
                cursor.execute("DELETE FROM sales_pipeline WHERE sales_agent = ?", (agent_to_remove,))
                conn.commit()
                st.warning(f"⚠️ Let go {agent_to_remove} (bottom performer)")
                
                # Clear investigation cache
                st.session_state.show_investigation = {}
            
            cursor.close()
            conn.close()
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button("♻️ Reset to Original Data", use_container_width=True):
        try:
            # Reload from CSV
            import pandas as pd
            df = pd.read_csv('sales_pipeline.csv')
            
            conn = sqlite3.connect('revenue_data.db')
            
            # Drop and recreate table
            conn.execute("DROP TABLE IF EXISTS sales_pipeline")
            df.to_sql('sales_pipeline', conn, index=False)
            
            conn.close()
            st.success("✅ Database reset to original data!")
            
            # Clear investigation cache
            st.session_state.show_investigation = {}
            
            st.rerun()
        except Exception as e:
            st.error(f"Error: {str(e)}")


# ============================================
# HELPER FUNCTIONS (defined first)
# ============================================

def query_database(query):
    """Execute SQL query and return results"""
    try:
        conn = sqlite3.connect('revenue_data.db')
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        return None

def get_demo_response(prompt):
    """Returns responses based on real data from MySQL"""
    prompt_lower = prompt.lower()
    
    if "pipeline" in prompt_lower or "open" in prompt_lower:
        results = query_database("""
            SELECT COUNT(*) as deal_count, SUM(close_value) as total_value
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
        """)
        if results and len(results) > 0:
            count, value = results[0]
            value = value or 0
            avg = value / count if count > 0 else 0
            return f"**Open Pipeline Analysis:**\n\n• Active deals: {count:,}\n• Total pipeline value: ${value:,.0f}\n• Average deal size: ${avg:,.0f}\n\nThis represents all currently open opportunities."
        
    elif "win rate" in prompt_lower or "won" in prompt_lower:
        results = query_database("""
            SELECT 
                deal_stage,
                COUNT(*) as count,
                SUM(close_value) as value
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
            GROUP BY deal_stage
        """)
        if results:
            won = next((r for r in results if r[0] == 'Won'), (None, 0, 0))
            lost = next((r for r in results if r[0] == 'Lost'), (None, 0, 0))
            total = won[1] + lost[1]
            win_rate = (won[1] / total * 100) if total > 0 else 0
            return f"**Win Rate Analysis:**\n\n• Deals won: {won[1]:,}\n• Deals lost: {lost[1]:,}\n• **Win rate: {win_rate:.1f}%**\n• Total won value: ${won[2]:,.0f}\n• Total lost potential: ${lost[2]:,.0f}"
    
    elif "rep" in prompt_lower or "agent" in prompt_lower or "performance" in prompt_lower:
        results = query_database("""
            SELECT 
                sales_agent,
                COUNT(*) as deals_won,
                SUM(close_value) as total_value
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY sales_agent
            ORDER BY total_value DESC
            LIMIT 5
        """)
        if results:
            response = "**Top 5 Sales Reps by Revenue:**\n\n"
            for i, (agent, deals, value) in enumerate(results, 1):
                avg = value / deals if deals > 0 else 0
                response += f"{i}. **{agent}**: {deals} won | ${value:,.0f} total | ${avg:,.0f} avg\n"
            return response
    
    elif "product" in prompt_lower:
        results = query_database("""
            SELECT 
                product,
                COUNT(*) as deals_won,
                SUM(close_value) as total_value,
                AVG(close_value) as avg_value
            FROM sales_pipeline
            WHERE deal_stage = 'Won'
            GROUP BY product
            ORDER BY total_value DESC
        """)
        if results:
            response = "**Product Performance (Won Deals):**\n\n"
            for product, deals, total, avg in results:
                response += f"• **{product}**: {deals} deals | ${total:,.0f} total | ${avg:,.0f} avg\n"
            return response
    
    elif "velocity" in prompt_lower or "cycle" in prompt_lower or "time" in prompt_lower:
        results = query_database("""
            SELECT 
                AVG(DATEDIFF(close_date, engage_date)) as avg_days,
                MIN(DATEDIFF(close_date, engage_date)) as min_days,
                MAX(DATEDIFF(close_date, engage_date)) as max_days
            FROM sales_pipeline
            WHERE deal_stage = 'Won' 
            AND close_date IS NOT NULL 
            AND engage_date IS NOT NULL
        """)
        if results and results[0][0]:
            avg_days, min_days, max_days = results[0]
            return f"**Sales Cycle Analysis:**\n\n• Average time to close: {int(avg_days)} days\n• Fastest deal: {int(min_days)} days\n• Longest deal: {int(max_days)} days"
    
    elif "forecast" in prompt_lower or "predict" in prompt_lower:
        results = query_database("""
            SELECT 
                COUNT(*) as open_count,
                SUM(close_value) as open_value
            FROM sales_pipeline
            WHERE deal_stage = 'Engaging'
        """)
        win_rate_result = query_database("""
            SELECT 
                SUM(CASE WHEN deal_stage = 'Won' THEN 1 ELSE 0 END) as won,
                COUNT(*) as total
            FROM sales_pipeline
            WHERE deal_stage IN ('Won', 'Lost')
        """)
        
        if results and win_rate_result:
            open_count, open_value = results[0]
            open_value = open_value or 0
            won_count, total_closed = win_rate_result[0]
            win_rate = won_count / total_closed if total_closed > 0 else 0
            
            forecast_deals = int(open_count * win_rate)
            forecast_value = int(open_value * win_rate)
            
            return f"**Pipeline Forecast:**\n\n• Open pipeline: {open_count:,} deals worth ${open_value:,.0f}\n• Historical win rate: {win_rate*100:.1f}%\n• **Forecasted wins: {forecast_deals:,} deals**\n• **Forecasted revenue: ${forecast_value:,.0f}**"
    
    elif "average" in prompt_lower or "avg" in prompt_lower:
        results = query_database("""
            SELECT AVG(close_value) as avg_deal
            FROM sales_pipeline
            WHERE deal_stage = 'Won' AND close_value > 0
        """)
        if results and results[0][0]:
            avg = results[0][0]
            return f"**Average Deal Size:** ${avg:,.0f}\n\nCalculated from all won deals."
    
    else:
        return f"**Try asking about:**\n• Pipeline value and open deals\n• Win rates and conversion\n• Top sales reps\n• Product performance\n• Sales cycle velocity\n• Revenue forecasting"

