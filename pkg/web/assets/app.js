// Live SSE listener and SVG topology renderer for Quorum Raft Cluster

let state = {
    nodes: [],
    links: [],
    kv: {},
    lastTerm: 0,
    leader: '--'
};

const nodeCoords = {};

function initTopologyCoords(nodes) {
    const radius = 160;
    const count = nodes.length || 5;
    nodes.forEach((node, idx) => {
        const angle = (idx * 2 * Math.PI / count) - (Math.PI / 2);
        nodeCoords[node.id] = {
            x: Math.round(radius * Math.cos(angle)),
            y: Math.round(radius * Math.sin(angle))
        };
    });
}

function renderTopology() {
    const linksGroup = document.getElementById('links-group');
    const nodesGroup = document.getElementById('nodes-group');
    if (!linksGroup || !nodesGroup) return;

    if (Object.keys(nodeCoords).length !== state.nodes.length) {
        initTopologyCoords(state.nodes);
    }

    // Render Links
    linksGroup.innerHTML = '';
    state.links.forEach(link => {
        const p1 = nodeCoords[link.from];
        const p2 = nodeCoords[link.to];
        if (!p1 || !p2) return;

        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', p1.x);
        line.setAttribute('y1', p1.y);
        line.setAttribute('x2', p2.x);
        line.setAttribute('y2', p2.y);
        line.setAttribute('class', link.blocked ? 'link-line link-severed' : 'link-line');
        linksGroup.appendChild(line);
    });

    // Render Nodes
    nodesGroup.innerHTML = '';
    state.nodes.forEach(node => {
        const pos = nodeCoords[node.id];
        if (!pos) return;

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('transform', `translate(${pos.x}, ${pos.y})`);

        // Outer pulse circle
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('r', '32');
        let roleClass = 'node-follower';
        if (node.role === 'Leader') roleClass = 'node-leader';
        if (node.role === 'Candidate') roleClass = 'node-candidate';
        circle.setAttribute('class', `node-circle ${roleClass}`);
        g.appendChild(circle);

        // Text ID
        const textID = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        textID.setAttribute('text-anchor', 'middle');
        textID.setAttribute('y', '-6');
        textID.setAttribute('fill', '#f8fafc');
        textID.setAttribute('font-size', '12px');
        textID.setAttribute('font-weight', 'bold');
        textID.textContent = node.id;
        g.appendChild(textID);

        // Text Role/Term
        const textMeta = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        textMeta.setAttribute('text-anchor', 'middle');
        textMeta.setAttribute('y', '12');
        textMeta.setAttribute('fill', '#94a3b8');
        textMeta.setAttribute('font-size', '9px');
        textMeta.textContent = `${node.role} · T${node.term}`;
        g.appendChild(textMeta);

        // Commit Index
        const textCommit = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        textCommit.setAttribute('text-anchor', 'middle');
        textCommit.setAttribute('y', '24');
        textCommit.setAttribute('fill', '#38bdf8');
        textCommit.setAttribute('font-size', '8px');
        textCommit.textContent = `CI: ${node.commit_index}`;
        g.appendChild(textCommit);

        nodesGroup.appendChild(g);
    });
}

function updateHUDMetrics() {
    let leader = '--';
    let maxTerm = 0;

    state.nodes.forEach(n => {
        if (n.role === 'Leader') leader = n.id;
        if (n.term > maxTerm) maxTerm = n.term;
    });

    document.getElementById('hud-leader').textContent = leader;
    document.getElementById('hud-term').textContent = maxTerm;
    document.getElementById('hud-quorum').textContent = `${Math.floor(state.nodes.length / 2) + 1} / ${state.nodes.length}`;

    // Update KV Table
    const tbody = document.getElementById('kv-tbody');
    const keys = Object.keys(state.kv);
    document.getElementById('kv-count').textContent = `${keys.length} KEYS`;

    if (keys.length === 0) {
        tbody.innerHTML = '<tr><td colspan="2" style="color: var(--text-dim); text-align: center;">No committed data</td></tr>';
    } else {
        tbody.innerHTML = '';
        keys.sort().forEach(k => {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td style="color: #38bdf8; font-weight: 600;">${k}</td><td>${state.kv[k]}</td>`;
            tbody.appendChild(tr);
        });
    }
}

function appendConsoleLog(msg) {
    const c = document.getElementById('event-console');
    if (!c) return;
    const entry = document.createElement('div');
    entry.className = 'console-entry';
    const timestamp = new Date().toLocaleTimeString();
    entry.textContent = `[${timestamp}] ${msg}`;
    c.appendChild(entry);
    c.scrollTop = c.scrollHeight;
}

// Connect SSE stream
function connectSSE() {
    const evtSource = new EventSource('/api/stream');

    evtSource.addEventListener('cluster_state', (e) => {
        try {
            const data = JSON.parse(e.data);
            state.nodes = data.nodes || [];
            state.links = data.links || [];
            state.kv = data.kv_store || {};
            renderTopology();
            updateHUDMetrics();
        } catch (err) {
            console.error('SSE JSON error', err);
        }
    });

    evtSource.onopen = () => {
        document.getElementById('stream-indicator').style.backgroundColor = 'var(--emerald)';
        document.getElementById('stream-status').textContent = 'LIVE';
        appendConsoleLog('SSE link established to cluster daemon');
    };

    evtSource.onerror = () => {
        document.getElementById('stream-indicator').style.backgroundColor = 'var(--crimson)';
        document.getElementById('stream-status').textContent = 'RETRYING';
    };
}

window.addEventListener('DOMContentLoaded', () => {
    connectSSE();
});
