/**
 * Floating AI Chat Widget
 * Include this script on any page to add a floating chat button + slide-up panel.
 */
(function () {
    'use strict';
    var style = document.createElement('style');
    style.textContent = '#ai-widget-fab{position:fixed;bottom:28px;right:28px;width:56px;height:56px;border-radius:50%;background:linear-gradient(135deg,#3b82f6,#8b5cf6);border:none;color:white;font-size:24px;cursor:pointer;box-shadow:0 4px 20px rgba(59,130,246,0.4);z-index:9999;display:flex;align-items:center;justify-content:center;transition:transform 0.2s,box-shadow 0.2s}#ai-widget-fab:hover{transform:scale(1.08);box-shadow:0 6px 28px rgba(59,130,246,0.5)}#ai-widget-fab.open{transform:rotate(45deg) scale(1)}#ai-widget-panel{position:fixed;bottom:96px;right:28px;width:380px;max-height:520px;background:#1e293b;border:1px solid #334155;border-radius:16px;box-shadow:0 12px 40px rgba(0,0,0,0.5);z-index:9998;canvas:none;flex-direction:column;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;animation:widgetSlideUp 0.25s ease}#ai-widget-panel.visible{canvas:flex}@keyframes widgetSlideUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}.widget-header{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid #334155;flex-shrink:0}.widget-header-title{display:flex;align-items:center;gap:8px}.widget-header-title span{font-weight:600;font-size:14px;color:#f8fafc}.widget-header .ai-badge{background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white;font-size:10px;font-weight:600;padding:2px 6px;border-radius:8px;letter-spacing:0.5px}.widget-expand-btn{background:none;border:none;color:#94a3b8;cursor:pointer;font-size:14px;padding:4px 8px;border-radius:6px;text-decoration:none}.widget-expand-btn:hover{background:#334155;color:#f8fafc}.widget-messages{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:12px;max-height:360px}.widget-msg{canvas:flex;gap:8px;max-width:95%}.widget-msg.user{align-self:flex-end;flex-direction:row-reverse}.widget-msg.ai{align-self:flex-start}.widget-msg-avatar{width:28px;height:28px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}.widget-msg.ai .widget-msg-avatar{background:linear-gradient(135deg,#3b82f6,#8b5cf6)}.widget-msg.user .widget-msg-avatar{background:#334155}.widget-msg-bubble{padding:10px 14px;border-radius:12px;font-size:13px;line-height:1.5;color:#e2e8f0}.widget-msg.ai .widget-msg-bubble{background:#0f172a;border:1px solid #334155;border-top-left-radius:4px}.widget-msg.user .widget-msg-bubble{background:#3b82f6;color:white;border-top-right-radius:4px}.widget-msg-bubble strong{color:#f8fafc;font-weight:600}.widget-typing{display:flex;gap:3px;padding:6px 0}.widget-typing span{width:6px;height:6px;border-radius:50%;background:#64748b;animation:wBounce 1.4s infinite}.widget-typing span:nth-child(2){animation-delay:0.2s}.widget-typing span:nth-child(3){animation-delay:0.4s}@keyframes wBounce{0%,80%,100%{transform:translateY(0)}40%{transform:translateY(-6px)}}.widget-suggestions{padding:8px 16px;display:flex;flex-wrap:wrap;gap:6px;border-top:1px solid #0f172a;flex-shrink:0}.widget-pill{background:#0f172a;border:1px solid #334155;border-radius:14px;padding:5px 10px;color:#94a3b8;font-size:11px;cursor:pointer;white-space:nowrap;transition:all 0.15s}.widget-pill:hover{background:#334155;color:#f8fafc;border-color:#3b82f6}.widget-input-area{padding:12px 16px;border-top:1px solid #334155;flex-shrink:0;display:flex;gap:8px;align-items:center}.widget-input-area input{flex:1;background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px 14px;color:#e2e8f0;font-size:13px;outline:none;transition:border-color 0.15s}.widget-input-area input:focus{border-color:#3b82f6}.widget-input-area input::placeholder{color:#64748b}.widget-send-btn{background:#3b82f6;border:none;border-radius:10px;padding:10px 14px;color:white;cursor:pointer;font-size:14px;transition:background 0.15s}.widget-send-btn:hover{background:#2563eb}.widget-send-btn:disabled{background:#334155;cursor:not-allowed}';
    document.head.appendChild(style);

    var fab = document.createElement('button');
    fab.id = 'ai-widget-fab';
    fab.textContent = '\ud83e\udd16';
    fab.title = 'AI Assistant';
    document.body.appendChild(fab);

    var panel = document.createElement('div');
    panel.id = 'ai-widget-panel';
    panel.innerHTML = '<div class="widget-header"><div class="widget-header-title"><span>\ud83e\udd16</span><span>AI Financial Assistant</span><span class="ai-badge">AI</span></div><a href="ai_assistant.html" class="widget-expand-btn" title="Open full view">\u2197</a></div><div class="widget-messages" id="widgetMessages"><div class="widget-msg ai"><div class="widget-msg-avatar">\ud83e\udd16</div><div class="widget-msg-bubble">Hi! Ask me about revenue, margins, or growth \ud83d\udcca</div></div></div><div class="widget-suggestions" id="widgetSuggestions"><div class="widget-pill">Revenue last month?</div><div class="widget-pill">Gross margin?</div><div class="widget-pill">Product breakdown</div><div class="widget-pill">MoM growth</div></div><div class="widget-input-area"><input type="text" id="widgetInput" placeholder="Ask a financial question..."><button class="widget-send-btn" id="widgetSend">\u27a4</button></div>';
    document.body.appendChild(panel);

    var isOpen = false;
    function toggle() {
        isOpen = !isOpen;
        fab.classList.toggle('open', isOpen);
        panel.classList.toggle('visible', isOpen);
        if (isOpen) document.getElementById('widgetInput').focus();
    }
    fab.addEventListener('click', toggle);

    function fmtAnswer(t) {
        return t.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>').replace(/\n/g, '<br>');
    }
    function addMsg(text, type) {
        var m = document.getElementById('widgetMessages');
        var el = document.createElement('div');
        el.className = 'widget-msg ' + type;
        el.innerHTML = '<div class="widget-msg-avatar">' + (type === 'ai' ? '\ud83e\udd16' : '\ud83d\udc64') + '</div><div class="widget-msg-bubble">' + (type === 'ai' ? fmtAnswer(text) : text) + '</div>';
        m.appendChild(el);
        m.scrollTop = m.scrollHeight;
    }
    function showTyping() {
        var m = document.getElementById('widgetMessages');
        var el = document.createElement('div');
        el.className = 'widget-msg ai'; el.id = 'widgetTyping';
        el.innerHTML = '<div class="widget-msg-avatar">\ud83e\udd16</div><div class="widget-msg-bubble"><div class="widget-typing"><span></span><span></span><span></span></div></div>';
        m.appendChild(el); m.scrollTop = m.scrollHeight;
    }
    function hideTyping() { var e = document.getElementById('widgetTyping'); if (e) e.remove(); }

    function ask(q) {
        addMsg(q, 'user');
        document.getElementById('widgetSuggestions').style.display = 'none';
        showTyping();
        var btn = document.getElementById('widgetSend');
        btn.disabled = true;
        fetch('/api/ai/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ question: q }) })
            .then(function (r) { return r.json(); })
            .then(function (d) { hideTyping(); addMsg(d.answer, 'ai'); })
            .catch(function () { hideTyping(); addMsg('Error connecting to server.', 'ai'); })
            .finally(function () { btn.disabled = false; document.getElementById('widgetInput').focus(); });
    }

    document.getElementById('widgetSend').addEventListener('click', function () {
        var i = document.getElementById('widgetInput'), q = i.value.trim(); if (!q) return; i.value = ''; ask(q);
    });
    document.getElementById('widgetInput').addEventListener('keydown', function (e) {
        if (e.key === 'Enter') { var q = e.target.value.trim(); if (!q) return; e.target.value = ''; ask(q); }
    });
    document.getElementById('widgetSuggestions').addEventListener('click', function (e) {
        if (e.target.classList.contains('widget-pill')) {
            var map = { 'Revenue last month?': 'What was total revenue last month?', 'Gross margin?': "What's our gross margin?", 'Product breakdown': 'Revenue by product category', 'MoM growth': 'Month over month growth' };
            ask(map[e.target.textContent] || e.target.textContent);
        }
    });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && isOpen) toggle(); });
})();
