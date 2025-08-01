# Kafka Transfers Application

Bu uygulama, Bitquery'nin Kafka stream'lerinden `solana.transfers.proto` topiğini dinleyen bir Go uygulamasıdır.

## Özellikler

- **Real-time Data Processing**: Solana transfer verilerini gerçek zamanlı olarak işler
- **Multi-partition Support**: Yüksek performans için çoklu partition desteği
- **Deduplication**: Aynı mesajların tekrar işlenmesini önler
- **Worker Pool**: Paralel mesaj işleme için worker pool
- **Statistics**: İşlenen mesaj sayısı ve hız istatistikleri

## Kurulum

### Gereksinimler

1. **Go**: Version >= 1.16
2. **Bitquery Kafka Access**: Bitquery Kafka broker'larına erişim
3. **Username ve Password**: Kafka broker'ları için kimlik doğrulama

### Bağımlılıkları Yükleme

```bash
go mod tidy
```

### Konfigürasyon

`config.yml` dosyasını düzenleyin:

```yaml
kafka:
  bootstrap.servers: "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092"
  security.protocol: "SASL_PLAINTEXT"
  sasl.mechanism: "SCRAM-SHA-512"
  sasl.username: "<your_username_here>"
  sasl.password: "<your_password_here>"
  group.id: "<username_group-number>"
  ssl.endpoint.identification.algorithm: "none"
  enable.auto.commit: false
consumer:
  topic: solana.transfers.proto
  partitioned: true
processor:
  buffer: 100
  workers: 8
log_level: "debug"
```

## Kullanım

### Uygulamayı Çalıştırma

```bash
go run .
```

veya

```bash
go build -o kafka-transfers.exe .
./kafka-transfers.exe
```

### Çıkış

Uygulamayı durdurmak için `Ctrl+C` tuşlarına basın.

## Dosya Yapısı

- `main.go`: Ana giriş noktası ve konfigürasyon yükleme
- `consumer.go`: Kafka consumer implementasyonu
- `processor.go`: Mesaj işleme ve worker pool yönetimi
- `statistics.go`: İstatistik toplama ve raporlama
- `config.yml`: Kafka ve uygulama konfigürasyonu

## Mesaj İşleme

Uygulama `solana.transfers.proto` topiğinden gelen protobuf mesajlarını işler. Her transfer mesajı şu bilgileri içerebilir:

- **From Address**: Gönderen adres
- **To Address**: Alıcı adres
- **Amount**: Transfer miktarı
- **Token Mint**: Token mint adresi
- **Transaction Signature**: İşlem imzası
- **Slot Number**: Slot numarası

## Performans

- **Buffer Size**: 100 mesaj (konfigüre edilebilir)
- **Worker Count**: 8 worker (konfigüre edilebilir)
- **Deduplication**: 240 saniye TTL ile LRU cache
- **Statistics**: Her 100 mesajda bir rapor

## Sorun Giderme

### Kafka Bağlantı Sorunları

1. `config.yml` dosyasındaki kimlik bilgilerini kontrol edin
2. Ağ bağlantınızı kontrol edin
3. Bitquery Kafka erişim izninizi kontrol edin

### Build Sorunları

Bu uygulama artık Sarama Kafka client'ını kullanıyor ve Windows'ta sorunsuz çalışır. Eğer hala sorun yaşıyorsanız:

```bash
# Go modülünü temizleyin
go clean -modcache
go mod tidy

# Veya WSL2 kullanarak Linux ortamında çalıştırın
```

## Geliştirme

### Yeni Topic Ekleme

1. `processor.go` dosyasında yeni handler ekleyin
2. `newProcessor` fonksiyonunda topic case'i ekleyin
3. Konfigürasyon dosyasında topic'i güncelleyin

### Protobuf Parsing

Protobuf mesajlarını parse etmek için:

1. `.proto` dosyasını indirin
2. `protoc` ile Go kodunu oluşturun
3. `transfersMessageHandler` fonksiyonunu güncelleyin

## Lisans

Bu proje MIT lisansı altında lisanslanmıştır. 