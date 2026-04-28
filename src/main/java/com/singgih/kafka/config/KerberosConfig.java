package com.singgih.kafka.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
@ConditionalOnExpression("'${kafka.security.protocol:PLAINTEXT}'.equals('SASL_PLAINTEXT') or '${kafka.security.protocol:PLAINTEXT}'.equals('SASL_SSL')")
public class KerberosConfig {

    private static final Logger log = LoggerFactory.getLogger(KerberosConfig.class);

    @Value("${kafka.security.krb5-conf-path}")
    private String krb5ConfPath;

    @Value("${kafka.security.keytab-path}")
    private String keytabPath;

    @Value("${kafka.security.principal}")
    private String principal;

    @Value("${kafka.security.kdc-host:}")
    private String kdcHost;

    /**
     * Memvalidasi keberadaan file krb5.conf dan keytab, lalu men-set system property
     * java.security.krb5.conf agar JVM menggunakan konfigurasi Kerberos yang benar.
     * Jika kafka.security.kdc-host dikonfigurasi, hostname 'kdc' di dalam krb5.conf
     * diganti secara otomatis dan disimpan ke file sementara — berguna saat aplikasi
     * berjalan di luar Docker network karena 'kdc' tidak dikenal oleh DNS host OS.
     */
    @PostConstruct
    public void init() {
        File krb5File = new File(krb5ConfPath);
        if (!krb5File.exists()) {
            throw new IllegalStateException(
                "File krb5.conf tidak ditemukan: " + krb5ConfPath +
                ". Pastikan path sudah benar di kafka.security.krb5-conf-path.");
        }

        File keytabFile = new File(keytabPath);
        if (!keytabFile.exists()) {
            throw new IllegalStateException(
                "File keytab tidak ditemukan: " + keytabPath +
                ". Jalankan: docker cp kdc:/keytabs/client.keytab " + keytabPath);
        }

        String resolvedKrb5Path = (kdcHost != null && !kdcHost.isBlank())
                ? buildResolvedKrb5Conf()
                : krb5ConfPath;

        System.setProperty("java.security.krb5.conf", resolvedKrb5Path);
        log.info("Kerberos aktif | principal: {} | krb5.conf: {} | keytab: {}",
                principal, resolvedKrb5Path, keytabPath);
    }

    /**
     * Membaca krb5.conf asli, mengganti semua kemunculan hostname 'kdc' (nama container
     * Docker) di bagian value konfigurasi dengan nilai kafka.security.kdc-host, lalu
     * menyimpan hasilnya ke file temporary yang dihapus otomatis saat JVM berhenti.
     *
     * @return path absolut ke file krb5.conf sementara yang sudah di-resolve
     */
    private String buildResolvedKrb5Conf() {
        try {
            String content = Files.readString(Path.of(krb5ConfPath));
            // Cocokkan 'kdc' hanya di bagian nilai (setelah '='), bukan di nama opsi
            String resolved = content.replaceAll("(=\\s*)kdc(?=:|\\s|\\r?\\n|$)", "$1" + kdcHost);
            Path tmp = Files.createTempFile("krb5-resolved-", ".conf");
            tmp.toFile().deleteOnExit();
            Files.writeString(tmp, resolved);
            log.info("krb5.conf: hostname 'kdc' diganti '{}', disimpan ke: {}", kdcHost, tmp);
            return tmp.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Gagal membuat krb5.conf sementara: " + e.getMessage(), e);
        }
    }
}
