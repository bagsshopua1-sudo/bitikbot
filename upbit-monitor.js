// upbit-monitor.js — моніторинг Upbit announcements через проксі
// Підключається до bot.js через callback

const axios = require('axios');
const { SocksProxyAgent } = require('socks-proxy-agent');

class UpbitMonitor {
  constructor(onListing) {
    this.onListing = onListing; // callback при новому лістингу
    this.seenIds = new Set();
    this.initialized = false;
    this.proxies = []; // масив проксі
    this.proxyIndex = 0;
    this.running = false;

    this.LISTING_KEYWORDS = [
      '추가', '신규 상장', '거래 지원', '디지털 자산 추가',
      '신규 거래지원', '거래지원 안내', 'Market Support',
      'Trade Support', 'listing', 'Listing'
    ];
    this.SKIP_KEYWORDS = [
      '입출금', '점검', '이벤트', '중단', '종료', '폐지', '유의',
      'delist', 'suspend', 'maintenance', 'deposit', 'withdrawal'
    ];
  }

  // Додати проксі
  addProxy(proxyUrl) {
    this.proxies.push(proxyUrl);
    console.log(`[Monitor] Added proxy ${this.proxies.length}: ${proxyUrl.split('@')[1] || proxyUrl}`);
  }

  // Отримати наступний проксі (round-robin)
  getNextProxy() {
    if (this.proxies.length === 0) return null;
    const proxy = this.proxies[this.proxyIndex];
    this.proxyIndex = (this.proxyIndex + 1) % this.proxies.length;
    return proxy;
  }

  // Витягти тікери з заголовку
  extractTickers(title) {
    const matches = title.match(/\(([A-Z]{2,10})\)/g) || [];
    const skip = ['KRW', 'BTC', 'USDT', 'USD', 'ETH', 'BNB', 'BUSD'];
    return [...new Set(matches.map(m => m.replace(/[()]/g, '')).filter(t => !skip.includes(t)))];
  }

  // Один запит до Upbit
  async fetchAnnouncements(proxyUrl) {
    const agent = proxyUrl ? new SocksProxyAgent(proxyUrl) : undefined;
    const res = await axios.get(
      'https://api-manager.upbit.com/api/v1/announcements?os=moweb&page=1&per_page=20&category=all',
      {
        httpsAgent: agent,
        headers: {
          'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15',
          'Accept': 'application/json',
          'Accept-Language': 'ko-KR,ko;q=0.9',
          'Origin': 'https://upbit.com',
          'Referer': 'https://upbit.com/service-center/notice',
        },
        timeout: 5000,
      }
    );
    return res.data?.data?.notices || [];
  }

  // Обробка одного тіку
  async tick() {
    try {
      const proxyUrl = this.getNextProxy();
      const notices = await this.fetchAnnouncements(proxyUrl);
      const seenAt = Date.now();

      if (!this.initialized) {
        notices.forEach(n => this.seenIds.add(n.id));
        console.log(`[Monitor] Initialized with ${this.seenIds.size} notices via proxy`);
        this.initialized = true;
        return;
      }

      for (const notice of notices) {
        if (this.seenIds.has(notice.id)) continue;
        this.seenIds.add(notice.id);

        const title = notice.title || '';
        console.log(`[Monitor] New notice: ${title}`);

        if (this.SKIP_KEYWORDS.some(w => title.includes(w))) {
          console.log(`[Monitor] Skip: ${title}`);
          continue;
        }

        if (!this.LISTING_KEYWORDS.some(w => title.includes(w))) {
          console.log(`[Monitor] Not listing: ${title}`);
          continue;
        }

        const tickers = this.extractTickers(title);
        if (tickers.length === 0) {
          console.log(`[Monitor] No tickers in: ${title}`);
          continue;
        }

        console.log(`[Monitor] LISTING! Tickers: ${tickers.join(', ')}`);
        
        // Викликаємо callback для кожного тікера
        for (const ticker of tickers) {
          this.onListing(ticker, seenAt);
        }
      }
    } catch(e) {
      if (e.response?.status === 429) {
        console.log(`[Monitor] 429 rate limit — proxy rotated`);
      } else if (e.response?.status === 403) {
        console.log(`[Monitor] 403 — proxy may be blocked`);
      } else {
        console.log(`[Monitor] Error: ${e.message}`);
      }
    }
  }

  // Запуск з кількома проксі паралельно
  start(intervalMs = 500) {
    if (this.running) return;
    this.running = true;

    if (this.proxies.length === 0) {
      console.log('[Monitor] No proxies configured — monitor disabled');
      return;
    }

    console.log(`[Monitor] Starting with ${this.proxies.length} proxies, interval=${intervalMs}ms`);
    console.log(`[Monitor] Total check rate: ${Math.round(1000 / (intervalMs / this.proxies.length))} req/sec`);

    // Запускаємо кожен проксі зі зсувом
    const offsetMs = Math.floor(intervalMs / this.proxies.length);
    
    this.proxies.forEach((proxy, index) => {
      setTimeout(() => {
        // Кожен проксі робить запит з інтервалом intervalMs
        setInterval(() => this.tick(), intervalMs);
        // Перший тік одразу
        this.tick();
        console.log(`[Monitor] Proxy ${index + 1} started with offset ${index * offsetMs}ms`);
      }, index * offsetMs);
    });
  }
}

module.exports = UpbitMonitor;
