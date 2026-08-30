// Конфигурация события (время московское)
const EVENT = {
    id: 'vkusno_i_tochka_genshin_2026',
    name: 'Вкусно и точка x Genshin Impact',
    startTime: new Date('2026-08-25T11:00:00+03:00')
};

// Состояние
let myClientId = localStorage.getItem('client_id');
if (!myClientId) {
    myClientId = generateUUID();
    localStorage.setItem('client_id', myClientId);
}

let myUsername = localStorage.getItem('username') || '';
if (myUsername && !myUsername.startsWith('@')) {
    myUsername = '@' + myUsername;
}

let selectedEvent = EVENT;
let selectedCity = null;
let participants = [];
let currentEventId = null;
let currentCity = null;
let mqttClient;
let timerInterval;

// Защита от множественных нажатий и rate limiting
let isSending = false;
let lastPublishTime = 0;
const publishMinInterval = 200; // мс
let joinCheckTimer = null;

// Tombstone: множество ID вышедших пользователей
let leftClients = new Set();

// Версия снапшота и время последнего изменения очереди
let snapshotVersion = 0;
let lastChangeTime = 0;

// Флаг, указывающий, что пользователь находится в процессе входа в очередь
let isJoining = false;

// DOM элементы
const screenDisclaimer = document.getElementById('screen-disclaimer');
const screenEvent = document.getElementById('screen-event');
const screenCity = document.getElementById('screen-city');
const screenQueue = document.getElementById('screen-queue');
const screenParticipants = document.getElementById('screen-participants');
const modalInfo = document.getElementById('modal-info');

const usernameInput = document.getElementById('username-input');
usernameInput.value = myUsername;

// Инициализация обработчиков
document.getElementById('btn-accept-disclaimer').addEventListener('click', acceptDisclaimer);
document.getElementById('btn-event-vkusno').addEventListener('click', () => selectEvent(EVENT));
document.getElementById('btn-next').addEventListener('click', () => {
    const username = usernameInput.value.trim().split('\n')[0].trim();
    if (!username) {
        alert('Обязательно введите username!');
        return;
    }
    myUsername = username.startsWith('@') ? username : '@' + username;
    localStorage.setItem('username', myUsername);
    showScreen('screen-city');
});
document.getElementById('btn-city-msk').addEventListener('click', () => selectCity('msk'));
document.getElementById('btn-city-spb').addEventListener('click', () => selectCity('spb'));
document.getElementById('btn-info').addEventListener('click', showInfoModal);
document.getElementById('btn-close-modal').addEventListener('click', () => modalInfo.classList.remove('active'));

// Кнопки очереди
document.getElementById('btn-join').addEventListener('click', joinQueue);
document.getElementById('btn-received').addEventListener('click', () => sendAction('received'));
document.getElementById('btn-here').addEventListener('click', () => sendAction('here'));
document.getElementById('btn-leave').addEventListener('click', () => sendAction('leave'));
document.getElementById('btn-participants').addEventListener('click', showParticipants);
document.getElementById('btn-back').addEventListener('click', () => showScreen('screen-queue'));
document.getElementById('btn-update-list').addEventListener('click', showParticipants);

// Модальное окно информации
const infoText = `Объявлены оффлайн-дни коллаборации Genshin Impact и «Вкусно и Точка» в Москве и Санкт-Петербурге!
Москва: 25 августа 12:00-18:00 ул. Азовская, д. 36
26 августа с 12:00-18:00 ул. Азовская, д. 36 и ул. Складочная 1стр 31 26го (2 точки)
Санкт-Петербург: 25 и 26 августа с 12:00-18:00 пл. Стачек, д. 9, стр. 1
Косплееры будут присутствовать на точках с 12:00 до 18:00`;
document.getElementById('modal-info-text').textContent = infoText;

// Функции переключения экранов
function showScreen(id) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    document.getElementById(id).classList.add('active');
}

function showInfoModal() {
    modalInfo.classList.add('active');
}

function selectEvent(event) {
    selectedEvent = event;
    document.getElementById('btn-event-vkusno').classList.add('selected');
}

function selectCity(city) {
    if (joinCheckTimer) {
        clearTimeout(joinCheckTimer);
        joinCheckTimer = null;
    }
    selectedCity = city;
    // Очищаем состояние для нового события/города
    leftClients.clear();
    snapshotVersion = 0;
    lastChangeTime = 0;
    participants = [];
    isJoining = false; // сбрасываем флаг входа
    connectMQTT();
    showScreen('screen-queue');
    updateQueueUI();
}

function acceptDisclaimer() {
    localStorage.setItem('disclaimer_accepted', '1');
    showScreen('screen-event');
}

// Генерация UUID
function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

// Подключение к MQTT
function connectMQTT() {
    if (mqttClient && mqttClient.connected) {
        mqttClient.end();
    }
    const brokerUrl = 'wss://broker.emqx.io:8084/mqtt';
    mqttClient = mqtt.connect(brokerUrl);

    mqttClient.on('connect', () => {
        currentEventId = selectedEvent.id;
        currentCity = selectedCity;
        const base = `event/${currentEventId}/${currentCity}`;
        mqttClient.subscribe(`${base}/join`);
        mqttClient.subscribe(`${base}/action`);
        mqttClient.subscribe(`${base}/snapshot`);
    });

    mqttClient.on('message', (topic, payload) => {
        const parts = topic.split('/');
        if (parts.length !== 4 || parts[0] !== 'event') return;
        const eventId = parts[1];
        const city = parts[2];
        const msgType = parts[3];
        if (eventId !== currentEventId || city !== currentCity) return;

        try {
            const message = JSON.parse(payload.toString());
            handleMessage(msgType, message);
        } catch (e) {
            console.error('Parse error', e);
        }
    });

    mqttClient.on('close', () => {
        console.log('MQTT disconnected');
        setTimeout(() => {
            if (selectedCity) connectMQTT();
        }, 5000);
    });

    if (timerInterval) clearInterval(timerInterval);
    timerInterval = setInterval(updateQueueUI, 1000);
}

// Отправка сообщения с учётом rate limiting
function publishWithRateLimit(topic, payload, callback) {
    const now = Date.now();
    const delay = Math.max(0, publishMinInterval - (now - lastPublishTime));
    setTimeout(() => {
        if (mqttClient && mqttClient.connected) {
            mqttClient.publish(topic, payload, { qos: 0 }, () => {
                lastPublishTime = Date.now();
                if (callback) callback();
            });
        } else {
            if (callback) callback();
        }
    }, delay);
}

function handleMessage(msgType, message) {
    if (msgType === 'join') {
        if (message.city === currentCity) {
            const participant = {
                client_id: message.client_id,
                username: message.username,
                status: 'waiting',
                ts: message.ts,
                nonce: message.nonce
            };
            addOrUpdateParticipant(participant);
            applyAfkRule();
            publishSnapshot();
        }
    } else if (msgType === 'action') {
        applyAction(message.client_id, message.type);
        applyAfkRule();
        publishSnapshot();
    } else if (msgType === 'snapshot') {
        loadSnapshot(message);
    }
}

function addOrUpdateParticipant(p) {
    const existing = participants.find(x => x.client_id === p.client_id);
    if (existing) {
        if (p.ts > existing.ts || (p.ts === existing.ts && p.nonce > existing.nonce)) {
            Object.assign(existing, p);
            lastChangeTime = Date.now();
        }
    } else {
        participants.push(p);
        lastChangeTime = Date.now();
    }
    // Если пользователь вернулся, убираем его из leftClients
    leftClients.delete(p.client_id);
}

function applyAction(clientId, actionType) {
    const p = participants.find(x => x.client_id === clientId);
    if (!p) return;

    if (actionType === 'received') {
        p.status = 'received';
        lastChangeTime = Date.now();
    } else if (actionType === 'here') {
        if (p.status === 'afk') {
            p.status = 'waiting';
            lastChangeTime = Date.now();
        }
    } else if (actionType === 'leave') {
        leftClients.add(clientId);
        participants = participants.filter(x => x.client_id !== clientId);
        lastChangeTime = Date.now();
    }
}

function applyAfkRule() {
    const sorted = [...participants].sort((a, b) => (a.ts - b.ts) || (a.nonce - b.nonce));
    let receivedCount = 0;
    for (let i = sorted.length - 1; i >= 0; i--) {
        const p = sorted[i];
        if (p.status === 'received') {
            receivedCount++;
        } else if (p.status === 'waiting') {
            if (receivedCount >= 5) {
                p.status = 'afk';
                lastChangeTime = Date.now();
            }
        }
    }
    participants = sorted;
}

function loadSnapshot(snapshot) {
    if (!snapshot || !snapshot.queue) return;
    if (snapshot.version <= snapshotVersion) return;

    snapshotVersion = snapshot.version;
    const now = Date.now();

    snapshot.queue.forEach(p => {
        if (leftClients.has(p.client_id)) return;

        const existing = participants.find(x => x.client_id === p.client_id);
        if (!existing) {
            participants.push({ ...p });
            lastChangeTime = now;
        } else {
            if ((p.ts > existing.ts) || (p.ts === existing.ts && p.nonce > existing.nonce)) {
                Object.assign(existing, p);
                lastChangeTime = now;
            }
        }
    });

    applyAfkRule();
}

function publishSnapshot() {
    if (!currentEventId || !currentCity) return;
    const snapshot = {
        version: Date.now(),
        last_ts: Date.now() / 1000,
        last_nonce: Math.floor(Math.random() * 1e15),
        queue: participants
    };
    publishWithRateLimit(`event/${currentEventId}/${currentCity}/snapshot`, JSON.stringify(snapshot), null);
}

// Функции для защиты от перекупов (локальное состояние)
function loadState() {
    try {
        return JSON.parse(localStorage.getItem('queue_state')) || {};
    } catch (e) {
        return {};
    }
}

function saveState(state) {
    localStorage.setItem('queue_state', JSON.stringify(state));
}

function getCooldownRemaining() {
    const state = loadState();
    const until = state.cooldownUntil || 0;
    return Math.max(0, Math.floor((until - Date.now()) / 1000));
}

function setCooldown(seconds) {
    const state = loadState();
    state.cooldownUntil = Date.now() + seconds * 1000;
    saveState(state);
}

function isRepeatFlagSet() {
    const state = loadState();
    return !!state.repeatFlag;
}

function setRepeatFlag() {
    const state = loadState();
    state.repeatFlag = true;
    saveState(state);
}

function hasDuplicateClientId() {
    return participants.some(p => p.client_id === myClientId && ['waiting', 'afk'].includes(p.status));
}

function hasDuplicateUsername(username) {
    return participants.some(p => p.username === username && ['waiting', 'afk'].includes(p.status));
}

function joinQueue() {
    if (isSending) return;
    if (!mqttClient || !mqttClient.connected) return;

    if (hasDuplicateClientId()) {
        document.getElementById('btn-join').textContent = 'Вы уже в очереди';
        document.getElementById('btn-join').disabled = true;
        return;
    }

    if (hasDuplicateUsername(myUsername)) {
        document.getElementById('btn-join').textContent = 'Этот username уже в очереди';
        document.getElementById('btn-join').disabled = true;
        return;
    }

    const cooldown = getCooldownRemaining();
    if (cooldown > 0) {
        document.getElementById('btn-join').disabled = true;
        document.getElementById('btn-join').textContent = `Подождите ${Math.floor(cooldown / 60)}:${String(cooldown % 60).padStart(2, '0')}`;
        return;
    }

    isSending = true;
    isJoining = true; // начинаем процесс входа
    document.getElementById('btn-join').disabled = true;
    document.getElementById('btn-join').textContent = 'Мы уже ставим вас в очередь...';

    const joinMsg = {
        client_id: myClientId,
        username: myUsername,
        city: currentCity,
        ts: Date.now() / 1000,
        nonce: Math.floor(Math.random() * 1e15)
    };

    publishWithRateLimit(`event/${currentEventId}/${currentCity}/join`, JSON.stringify(joinMsg), () => {
        isSending = false;
        addOrUpdateParticipant({
            client_id: myClientId,
            username: myUsername,
            status: 'waiting',
            ts: joinMsg.ts,
            nonce: joinMsg.nonce
        });
        applyAfkRule();
        updateQueueUI();
        joinCheckTimer = setTimeout(checkJoinResult, 10000);
    });
}

function checkJoinResult() {
    const myParticipant = participants.find(p => p.client_id === myClientId);
    if (myParticipant) {
        isJoining = false; // вход подтверждён
        joinCheckTimer = null;
        updateQueueUI();
    } else {
        const joinMsg = {
            client_id: myClientId,
            username: myUsername,
            city: currentCity,
            ts: Date.now() / 1000,
            nonce: Math.floor(Math.random() * 1e15)
        };
        publishWithRateLimit(`event/${currentEventId}/${currentCity}/join`, JSON.stringify(joinMsg), () => {
            joinCheckTimer = setTimeout(checkJoinResult, 10000);
        });
    }
}

function sendAction(actionType) {
    if (isSending) return;
    if (!mqttClient || !mqttClient.connected) return;

    const myParticipant = participants.find(p => p.client_id === myClientId);
    if (!myParticipant) return;

    if (actionType === 'received' && !['waiting', 'afk'].includes(myParticipant.status)) return;
    if (actionType === 'here' && myParticipant.status !== 'afk') return;
    if (actionType === 'leave' && !myParticipant) return;

    isSending = true;

    const actionMsg = {
        client_id: myClientId,
        type: actionType,
        ts: Date.now() / 1000,
        nonce: Math.floor(Math.random() * 1e15),
        city: currentCity
    };

    publishWithRateLimit(`event/${currentEventId}/${currentCity}/action`, JSON.stringify(actionMsg), () => {
        isSending = false;
        if (actionType === 'received' || actionType === 'here') {
            applyAction(myClientId, actionType);
            if (actionType === 'received') {
                setCooldown(300);
                setRepeatFlag();
            }
            publishSnapshot();
        } else if (actionType === 'leave') {
            leftClients.add(myClientId);
            participants = participants.filter(p => p.client_id !== myClientId);
            lastChangeTime = Date.now();
            if (joinCheckTimer) {
                clearTimeout(joinCheckTimer);
                joinCheckTimer = null;
            }
            isJoining = false; // после выхода сбрасываем флаг
            publishSnapshot();
        }
        applyAfkRule();
        updateQueueUI();
    });
}

function updateQueueUI() {
    const now = new Date();
    const start = EVENT.startTime;
    const diff = Math.max(0, start - now);
    const totalSeconds = Math.floor(diff / 1000);
    const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
    const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    document.getElementById('queue-event-name').textContent = EVENT.name;
    document.getElementById('queue-timer').textContent = now < start ? `До начала: ${hours}:${minutes}:${seconds}` : 'Очередь открыта';

    const myParticipant = participants.find(p => p.client_id === myClientId);
    const status = myParticipant ? myParticipant.status : null;
    let statusText = 'Вы не в очереди';
    let positionText = '';
    const statusMap = { waiting: 'Ожидает', received: 'Получил', afk: 'Неактивен' };

    if (status) {
        if (status === 'waiting' && isRepeatFlagSet()) {
            statusText = 'Ваш статус: Ожидает повторно';
        } else {
            statusText = `Ваш статус: ${statusMap[status]}`;
        }

        const activeParticipants = participants.filter(p => p.status === 'waiting').sort((a,b) => (a.ts - b.ts) || (a.nonce - b.nonce));
        const pos = activeParticipants.findIndex(p => p.client_id === myClientId);
        if (pos !== -1) {
            if (isJoining) {
                positionText = 'Синхронизация очереди...';
            } else {
                positionText = `Ваш номер: #${pos + 1}`;
            }
        }
    }

    document.getElementById('queue-status').textContent = statusText;
    document.getElementById('queue-position').textContent = positionText;

    const joinBtn = document.getElementById('btn-join');
    const receivedBtn = document.getElementById('btn-received');
    const hereBtn = document.getElementById('btn-here');
    const leaveBtn = document.getElementById('btn-leave');

    joinBtn.disabled = false;
    receivedBtn.disabled = true;
    hereBtn.disabled = true;
    leaveBtn.disabled = true;
    joinBtn.textContent = 'Встать в очередь';

    if (now < start) {
        joinBtn.disabled = true;
        joinBtn.textContent = `Очередь откроется через ${hours}:${minutes}:${seconds}`;
        return;
    }

    const cooldown = getCooldownRemaining();
    if (cooldown > 0) {
        joinBtn.disabled = true;
        joinBtn.textContent = `Подождите ${Math.floor(cooldown / 60)}:${String(cooldown % 60).padStart(2, '0')}`;
        return;
    }

    if (status === 'waiting') {
        joinBtn.disabled = true;
        joinBtn.textContent = 'Вы в очереди';
        receivedBtn.disabled = false;
        leaveBtn.disabled = false;
    } else if (status === 'afk') {
        joinBtn.disabled = true;
        joinBtn.textContent = 'Вы неактивны';
        receivedBtn.disabled = false;
        hereBtn.disabled = false;
        leaveBtn.disabled = false;
    } else if (status === 'received') {
        joinBtn.disabled = false;
        joinBtn.textContent = 'Встать в очередь снова';
    } else {
        joinBtn.disabled = false;
        joinBtn.textContent = 'Встать в очередь';
    }

    if (isSending) {
        joinBtn.disabled = true;
        receivedBtn.disabled = true;
        hereBtn.disabled = true;
        leaveBtn.disabled = true;
        if (status === null) {
            joinBtn.textContent = 'Мы уже ставим вас в очередь...';
        }
    }
}

function showParticipants() {
    const list = document.getElementById('participants-list');
    list.innerHTML = '';
    const sorted = [...participants].sort((a,b) => (a.ts - b.ts) || (a.nonce - b.nonce));
    const statusMap = { waiting: 'Ожидает', received: 'Получил', afk: 'Неактивен' };
    sorted.forEach((p, i) => {
        const btn = document.createElement('button');
        btn.className = 'participant-button';
        btn.textContent = `${i+1}. ${p.username} (${statusMap[p.status] || p.status})`;
        btn.addEventListener('click', () => {
            let username = p.username;
            if (username.startsWith('@')) username = username.slice(1);
            window.open(`https://t.me/${username}`, '_blank');
        });
        list.appendChild(btn);
    });
    showScreen('screen-participants');
}

// Регистрация Service Worker
if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
        navigator.serviceWorker.register('service-worker.js')
            .then(reg => console.log('SW registered'))
            .catch(err => console.log('SW registration failed', err));
    });
}

// При запуске проверяем, был ли принят отказ
if (localStorage.getItem('disclaimer_accepted') === '1') {
    showScreen('screen-event');
} else {
    showScreen('screen-disclaimer');
}

// Устанавливаем начальное состояние кнопок
updateQueueUI();
