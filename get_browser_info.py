# get_browser_info.py
"""
Get browser user-agent info for Streamlit apps
Uses JavaScript injection to capture real browser details
"""

import streamlit as st
import streamlit.components.v1 as components

def get_browser_user_agent():
    """
    Get the actual browser user agent using JavaScript.
    Returns user agent string or 'Streamlit App' as fallback.
    """
    
    # Inject JavaScript to get user agent
    user_agent_js = """
    <script>
        // Get user agent
        var userAgent = navigator.userAgent;
        
        // Send to parent (Streamlit)
        window.parent.postMessage({
            type: 'streamlit:setComponentValue',
            value: userAgent
        }, '*');
    </script>
    """
    
    try:
        # Try to get user agent from JavaScript
        user_agent = components.html(user_agent_js, height=0)
        
        if user_agent:
            return user_agent
    except:
        pass
    
    # Fallback
    return "Streamlit App (Browser detection unavailable)"