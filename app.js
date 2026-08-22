// Конфигурация события (время московское)
const EVENT = {
    id: 'rostics_hsr_2026',
    name: 'Rostics x Honkai star rail',
    startTime: new Date('2026-08-16T09:00:00+03:00')
};

// Состояние
let myClientId = localStorage.getItem('client_id');
if (!myClientId) {
    myClientId = generateUUID();
    localStorage.setItem('client_id', myClientId);
}

let myUsername = localStorage.getItem('username') || '';
let selectedEvent = EVENT;
let selectedCity = null;
let participants = [];
let currentEventId = null;
let currentCity = null;
let mqttClient;
let timerInterval;

// DOM элементы
const screenEvent = document.getElementById('screen-event');
const screenCity = document.getElementById('screen-city');
const screenQueue = document.getElementById('screen-queue');
const screenParticipants = document.getElementById('screen-participants');
const modalInfo = document.getElementById('modal-info');

const usernameInput = document.getElementById('username-input');
usernameInput.value = myUsername;
usernameInput.placeholder = 'Введите ваш Telegram username (например, @ivan)\nВы можете написать после @ всё что хотите, если нет желания чтобы другие знали ваш аккаунт в telegram';

// Инициализация
document.getElementById('btn-event-rostics').addEventListener('click', () => selectEvent(EVENT));
document.getElementById('btn-next').addEventListener('click', () => {
    const username = usernameInput.value.trim().split('\n')[0].trim();
    if (!username) {
        alert('Обязательно введите username!');
        return;
    }
    myUsername = username;
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
document.getElementById('btn-participants').addEventListener('click', () => showScreen('screen-participants'));
document.getElementById('btn-back').addEventListener('click', () => showScreen('screen-queue'));

// Модальное окно информации
const infoText = `Ивент дни в Санкт-Петербурге и Москве 15 и 16 августа
Адрес в Москве: Rostics Парк Горького
Адрес в СПБ: Rostics Каменноостровский пр., д.37
Экспресс комбо считаются только в одном чеке. Вам понадобится предъявить чек/коробки.
1 комбо - голографический билет
2 комбо - билет + стикерпак
3 комбо - билет + стикерпак + акриловый стенд
Если взять латте в комбо, можно получить капхоледр с Химеко Нова
Если сделать фото с косплеерами и выложить его с хештегом #HSRxROSTICS вы получите открытку коллаборации
Также не забудьте привязать аккаунт в приложении, чтобы получить нефрит
Раньше была официальная информация что будет розыгрыш мерча за покупку 1 комбо, но по всей видимости розыгрыш отменили
Первые 50 косплееров (не закос) получили акриловые стенды (по слухам количество увеличили до 100)
Мерч выдают с 10.00 до 19.00 включительно
Фотографии мерча: https://t.me/hoyoverse_events_russia/1583?single`;
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
    // Визуально выделяем кнопку
    document.querySelectorAll('.event-button').forEach(btn => btn.classList.remove('selected'));
    document.getElementById('btn-event-rostics').classList.add('selected');
}

function selectCity(city) {
    selectedCity = city;
    connectMQTT();
    showScreen('screen-queue');
    updateQueueUI();
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
        // Запрос актуального снапшота
        // (будет автоматически получен из retained топика)
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
        // Попытка переподключения через 5 секунд
        setTimeout(() => {
            if (selectedCity) connectMQTT();
        }, 5000);
    });

    // Таймер обновления UI
    if (timerInterval) clearInterval(timerInterval);
    timerInterval = setInterval(updateQueueUI, 1000);
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
        }
    } else {
        participants.push(p);
    }
}

function applyAction(clientId, actionType) {
    const p = participants.find(x => x.client_id === clientId);
    if (!p) return;
    if (actionType === 'received') {
        p.status = 'received';
    } else if (actionType === 'here') {
        if (p.status === 'afk') p.status = 'waiting';
    } else if (actionType === 'leave') {
        participants = participants.filter(x => x.client_id !== clientId);
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
            if (receivedCount >= 5) p.status = 'afk';
        }
    }
    participants = sorted;
}

function loadSnapshot(snapshot) {
    if (!snapshot || !snapshot.queue) return;
    participants = snapshot.queue.map(p => ({ ...p }));
}

function publishSnapshot() {
    if (!currentEventId || !currentCity) return;
    const snapshot = {
        version: Date.now(),
        last_ts: Date.now(),
        last_nonce: Math.floor(Math.random() * 1e9),
        queue: participants
    };
    mqttClient.publish(`event/${currentEventId}/${currentCity}/snapshot`, JSON.stringify(snapshot), { retain: true, qos: 0 });
}

function joinQueue() {
    if (!mqttClient || !mqttClient.connected) return;
    const joinMsg = {
        client_id: myClientId,
        username: myUsername,
        city: currentCity,
        ts: Math.floor(Date.now() / 1000),
        nonce: Math.floor(Math.random() * 1e15)
    };
    mqttClient.publish(`event/${currentEventId}/${currentCity}/join`, JSON.stringify(joinMsg), { qos: 0 });
    // Локально добавляем себя
    addOrUpdateParticipant({
        client_id: myClientId,
        username: myUsername,
        status: 'waiting',
        ts: joinMsg.ts,
        nonce: joinMsg.nonce
    });
    applyAfkRule();
    updateQueueUI();
}

function sendAction(actionType) {
    if (!mqttClient || !mqttClient.connected) return;
    const actionMsg = {
        client_id: myClientId,
        type: actionType,
        ts: Math.floor(Date.now() / 1000),
        nonce: Math.floor(Math.random() * 1e15),
        city: currentCity
    };
    mqttClient.publish(`event/${currentEventId}/${currentCity}/action`, JSON.stringify(actionMsg), { qos: 0 });
    // Локально применяем действие
    if (actionType === 'received' || actionType === 'here') applyAction(myClientId, actionType);
    if (actionType === 'leave') {
        participants = participants.filter(p => p.client_id !== myClientId);
    }
    applyAfkRule();
    updateQueueUI();
}

function updateQueueUI() {
    // Таймер
    const now = new Date();
    const start = EVENT.startTime;
    const diff = Math.max(0, start - now);
    const totalSeconds = Math.floor(diff / 1000);
    const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
    const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    document.getElementById('queue-event-name').textContent = `Событие: ${EVENT.name}`;
    document.getElementById('queue-timer').textContent = now < start ? `До начала: ${hours}:${minutes}:${seconds}` : 'Очередь открыта';

    const myParticipant = participants.find(p => p.client_id === myClientId);
    const status = myParticipant ? myParticipant.status : null;
    let statusText = 'Вы не в очереди';
    let positionText = '';
    const statusMap = { waiting: 'Ожидает', received: 'Получил', afk: 'Неактивен' };

    if (status) {
        statusText = `Ваш статус: ${statusMap[status]}`;
        const activeParticipants = participants.filter(p => p.status === 'waiting').sort((a,b) => (a.ts - b.ts) || (a.nonce - b.nonce));
        const pos = activeParticipants.findIndex(p => p.client_id === myClientId);
        if (pos !== -1) positionText = `Ваш номер: ${pos + 1}`;
    }

    document.getElementById('queue-status').textContent = statusText;
    document.getElementById('queue-position').textContent = positionText;

    // Кнопки
    const joinBtn = document.getElementById('btn-join');
    const receivedBtn = document.getElementById('btn-received');
    const hereBtn = document.getElementById('btn-here');
    const leaveBtn = document.getElementById('btn-leave');

    joinBtn.disabled = false;
    receivedBtn.disabled = true;
    hereBtn.disabled = true;
    leaveBtn.disabled = true;
    joinBtn.textContent = 'Встать в очередь';

    if (now < start && !status) {
        joinBtn.disabled = true;
        joinBtn.textContent = `Очередь откроется через ${hours}:${minutes}:${seconds}`;
    } else if (status === 'waiting') {
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
        joinBtn.textContent = 'Встать в очередь снова';
        joinBtn.disabled = false;
    }
}

// Обновление списка участников
document.getElementById('btn-participants').addEventListener('click', () => {
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
});

// Регистрация Service Worker
if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
        navigator.serviceWorker.register('service-worker.js')
            .then(reg => console.log('SW registered'))
            .catch(err => console.log('SW registration failed', err));
    });
    }
