import streamlit as st
import anthropic
import os
from dotenv import load_dotenv
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Revenue Intelligence Chatbot",
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
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "demo_mode" not in st.session_state:
    st.session_state.demo_mode = True

# ============================================
# HELPER FUNCTIONS (defined first)
# ============================================

def query_database(query):
    """Execute SQL query and return results"""
    try:
        conn = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE', 'revenue_ops')
        )
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

def get_claude_response(prompt):
    """Calls Claude API for real intelligence"""
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return "❌ API key not found."
        
        client = anthropic.Anthropic(api_key=api_key)
        
        system_prompt = """You are a Revenue Operations analyst. Help sales leaders analyze pipeline, 
        forecast deals, and make data-driven decisions. Keep responses concise and actionable."""
        
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return message.content[0].text
        
    except Exception as e:
        return f"❌ Error: {str(e)}"

# ============================================
# MAIN UI
# ============================================

# Title
st.title("📊 Revenue Intelligence Chatbot")
st.caption("Ask questions about your pipeline, deals, and revenue metrics")
st.markdown("**Built by Carlos Gonzalez** | [LinkedIn](https://linkedin.com/in/carlosgonzalez01)")

# KPI Dashboard
st.divider()

try:
    stats_query = query_database("""
        SELECT 
            deal_stage,
            COUNT(*) as count,
            SUM(close_value) as value
        FROM sales_pipeline
        GROUP BY deal_stage
    """)
    
    if stats_query:
        stats_df = pd.DataFrame(stats_query, columns=['Stage', 'Count', 'Value'])
        stats_df['Value'] = stats_df['Value'].fillna(0)
        
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            won_deals = stats_df[stats_df['Stage'] == 'Won']['Count'].sum()
            won_value = stats_df[stats_df['Stage'] == 'Won']['Value'].sum()
            st.metric("💰 Won Deals", f"{int(won_deals):,}", f"${int(won_value):,.0f}")
        
        with col2:
            lost_deals = stats_df[stats_df['Stage'] == 'Lost']['Count'].sum()
            total_closed = won_deals + lost_deals
            win_rate = (won_deals / total_closed * 100) if total_closed > 0 else 0
            st.metric("📈 Win Rate", f"{win_rate:.1f}%", f"{int(won_deals):,} / {int(total_closed):,}")
        
        with col3:
            open_deals = stats_df[stats_df['Stage'] == 'Engaging']['Count'].sum()
            open_value = stats_df[stats_df['Stage'] == 'Engaging']['Value'].sum()
            st.metric("🎯 Open Pipeline", f"{int(open_deals):,} deals", f"${int(open_value):,.0f}")
        
        with col4:
            avg_deal = won_value / won_deals if won_deals > 0 else 0
            st.metric("💵 Avg Deal Size", f"${int(avg_deal):,.0f}", "Won deals")
        
        # Charts
        st.divider()
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Deal distribution pie chart
            fig_pie = px.pie(
                stats_df,
                values='Count',
                names='Stage',
                title='Deal Distribution by Stage',
                color='Stage',
                color_discrete_map={'Won': '#00cc66', 'Lost': '#ff6666', 'Engaging': '#ffaa00'}
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#171a20')
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with chart_col2:
            fig_revenue = px.bar(
                stats_df,
                x='Stage',
                y='Value',
                title='Revenue by Stage',
                labels={'Value': 'Total Value ($)', 'Stage': 'Deal Stage'},
                color='Stage',
                color_discrete_map={'Won': '#00cc66', 'Lost': '#ff6666', 'Engaging': '#ffaa00'}
            )
            fig_revenue.update_layout(
                height=300,
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=False,
                yaxis=dict(rangemode='tozero'),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#171a20'),
                xaxis=dict(gridcolor='#f0f0f0'),
                yaxis2=dict(gridcolor='#f0f0f0')
            )
            st.plotly_chart(fig_revenue, use_container_width=True)

except Exception as e:
    st.error(f"Dashboard error: {str(e)}")

st.divider()

# Welcome message - only show if no chat history
if len(st.session_state.messages) == 0:
    col1, col2 = st.columns([1, 3])
    
    with col1:
        try:
            st.image("avatar.png", width=150)
        except:
            st.write("👤")
    
    with col2:
        st.markdown("### 👋 Hey, I'm Carlos Gonzalez")
        st.markdown("**Your next Revenue Operations Analyst**")
        st.markdown("I built this AI-powered chatbot to show you what I can do. Ask me anything about the sales pipeline data below!")
    
    st.divider()
    
    # Example questions
    st.markdown("#### 💬 Try asking me:")
    
    q_col1, q_col2 = st.columns(2)
    
    with q_col1:
        st.markdown("**📊 Pipeline Analysis:**")
        st.markdown("• *What's our open pipeline value?*")
        st.markdown("• *Show me our win rate*")
        st.markdown("• *What's the average deal size?*")
        
        st.markdown("**🏆 Performance:**")
        st.markdown("• *Who are the top sales reps?*")
        st.markdown("• *Show me product performance*")
        st.markdown("• *How long is our sales cycle?*")
    
    with q_col2:
        st.markdown("**🔮 Forecasting:**")
        st.markdown("• *What's our revenue forecast?*")
        st.markdown("• *Predict our pipeline conversion*")
        
        st.markdown("**🎮 Want to see it live?**")
        st.markdown("Use the **Admin Panel** on the left to:")
        st.markdown("• Close deals in bulk (10-100x)")
        st.markdown("• Add new opportunities")
        st.markdown("• Mark deals as lost")
        st.markdown("*Then ask me the same question again and watch the numbers change in real-time!*")
    
    st.divider()

# Chat interface
for message in st.session_state.messages:
    if message["role"] == "assistant":
        with st.chat_message("assistant", avatar="avatar.png"):
            st.markdown(message["content"])
    else:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

if prompt := st.chat_input("Ask a question about your revenue data..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant", avatar="avatar.png"):
        with st.spinner("Analyzing..."):
            if st.session_state.demo_mode:
                response = get_demo_response(prompt)
            else:
                response = get_claude_response(prompt)
        
        st.markdown(response)
    
    st.session_state.messages.append({"role": "assistant", "content": response})

# Sidebar
with st.sidebar:
    st.header("💼 Carlos Gonzalez")
    st.caption("Revenue Operations Analyst")
    st.write("Built this tool to demonstrate:")
    st.write("• AI-powered analytics")
    st.write("• Real-time data systems")
    st.write("• RevOps automation")
    
    st.info("👋 **Looking to hire a RevOps Analyst?** [Connect with me on LinkedIn!](https://linkedin.com/in/carlosgonzalez01)")
    
    st.divider()
    
    st.header("About This Tool")
    st.write("Analyzes 8,800+ real B2B deals from Kaggle dataset")
    
    st.divider()
    
    mode = st.radio(
        "Mode",
        ["Demo (Free)", "Live API (Costs $)"],
        index=0 if st.session_state.demo_mode else 1
    )
    st.session_state.demo_mode = (mode == "Demo (Free)")
    
    if st.session_state.demo_mode:
        st.success("💰 Demo Mode - No API costs")
    else:
        st.warning("💸 Live Mode - Using API credits")
    
    st.divider()
    
    # Admin Panel
    st.header("🎮 Admin Panel")
    st.caption("Simulate real-time events")
    
    multiplier = st.selectbox("Multiplier", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    
    if st.button(f"💰 Close {multiplier} x $50K Deals", use_container_width=True):
        try:
            conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE', 'revenue_ops')
            )
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT opportunity_id FROM sales_pipeline WHERE deal_stage = 'Engaging' LIMIT {multiplier}")
            results = cursor.fetchall()
            
            if results:
                for (opp_id,) in results:
                    cursor.execute("""
                        UPDATE sales_pipeline 
                        SET deal_stage = 'Won', close_value = 50000, close_date = CURDATE()
                        WHERE opportunity_id = %s
                    """, (opp_id,))
                
                conn.commit()
                st.success(f"✅ Closed {len(results)} deals for ${len(results)*50000:,}!")
                st.balloons()
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button(f"❌ Mark {multiplier} Deals as Lost", use_container_width=True):
        try:
            conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE', 'revenue_ops')
            )
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT opportunity_id FROM sales_pipeline WHERE deal_stage = 'Engaging' LIMIT {multiplier}")
            results = cursor.fetchall()
            
            if results:
                for (opp_id,) in results:
                    cursor.execute("UPDATE sales_pipeline SET deal_stage = 'Lost', close_date = CURDATE() WHERE opportunity_id = %s", (opp_id,))
                
                conn.commit()
                st.warning(f"Marked {len(results)} deals as lost")
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.button(f"➕ Add {multiplier} x $75K Opportunities", use_container_width=True):
        try:
            import random
            import string
            
            conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE', 'revenue_ops')
            )
            cursor = conn.cursor()
            
            cursor.execute("SELECT DISTINCT sales_agent FROM sales_pipeline")
            agents = [row[0] for row in cursor.fetchall()]
            
            for _ in range(multiplier):
                opp_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                agent = random.choice(agents)
                
                cursor.execute("""
                    INSERT INTO sales_pipeline 
                    (opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
                    VALUES (%s, %s, 'GTXPro', 'New Demo Account', 'Engaging', CURDATE(), NULL, 75000)
                """, (opp_id, agent))
            
            conn.commit()
            st.success(f"✅ Added {multiplier} opportunities worth ${multiplier*75000:,}!")
            
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    st.divider()
    
    # Quick Stats
    st.subheader("📊 Quick Stats")
    try:
        stats = query_database("SELECT deal_stage, COUNT(*), SUM(close_value) FROM sales_pipeline GROUP BY deal_stage")
        if stats:
            for stage, count, value in stats:
                value = value or 0
                st.metric(stage, f"{count:,} deals", f"${value:,.0f}")
    except:
        pass
    
    st.divider()
    
    api_status = "✅" if os.getenv("ANTHROPIC_API_KEY") else "❌"
    db_status = "✅" if os.getenv("MYSQL_PASSWORD") else "❌"
    st.write(f"**API:** {api_status} | **DB:** {db_status}")
    
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()
