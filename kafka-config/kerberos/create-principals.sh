#!/bin/bash
set -e

REALM="CONFLUENT.LOCAL"
KEYTAB_DIR="/keytabs"

echo "============================================"
echo " Kerberos KDC Setup"
echo " Realm: $REALM"
echo "============================================"

# Hapus marker lama agar healthcheck tidak terpenuhi sebelum waktunya
rm -f "$KEYTAB_DIR/.kdc_ready"

# -----------------------------------------------------------------
# Inisialisasi database KDC (hanya saat pertama kali)
# -----------------------------------------------------------------
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "[1/5] Membuat KDC database baru..."
    kdb5_util create -s -P "kdc_master_key_confluent" -r "$REALM"
    echo "      Selesai."
else
    echo "[1/5] KDC database sudah ada, melewati inisialisasi."
fi

# -----------------------------------------------------------------
# Start KDC daemon
# -----------------------------------------------------------------
echo "[2/5] Menjalankan krb5kdc..."
/usr/sbin/krb5kdc -n &
KDC_PID=$!
sleep 2

# -----------------------------------------------------------------
# Start kadmind
# -----------------------------------------------------------------
echo "[3/5] Menjalankan kadmind..."
/usr/sbin/kadmind -nofork &
KADMIND_PID=$!
sleep 2

# -----------------------------------------------------------------
# Helper: buat service principal + export keytab
# -----------------------------------------------------------------
create_service_keytab() {
    local principal=$1
    local keytab_file=$2
    echo "  -> $principal"
    kadmin.local -q "addprinc -randkey $principal" 2>/dev/null || true
    kadmin.local -q "ktadd -k $KEYTAB_DIR/$keytab_file $principal"
    chmod 644 "$KEYTAB_DIR/$keytab_file"
}

# -----------------------------------------------------------------
# Buat principals
# -----------------------------------------------------------------
echo "[4/5] Membuat principals dan keytabs..."

# Kafka broker principal
create_service_keytab "kafka/kafka@$REALM" "kafka.keytab"

# Client principal untuk testing (password-based + keytab)
echo "  -> client@$REALM  (password: client123)"
kadmin.local -q "addprinc -pw client123 client@$REALM" 2>/dev/null || true
kadmin.local -q "ktadd -k $KEYTAB_DIR/client.keytab client@$REALM"
chmod 644 "$KEYTAB_DIR/client.keytab"

# Admin principal (untuk operasional KDC)
kadmin.local -q "addprinc -pw admin admin/admin@$REALM" 2>/dev/null || true

echo ""
echo "[5/5] Ringkasan:"
echo "  Principals terdaftar:"
kadmin.local -q "listprincs" | sed 's/^/    /'
echo ""
echo "  Keytab files di $KEYTAB_DIR:"
ls -lh "$KEYTAB_DIR/"*.keytab 2>/dev/null | sed 's/^/    /' || echo "    (tidak ada)"

# -----------------------------------------------------------------
# Signal bahwa KDC sudah siap (dibaca oleh healthcheck)
# -----------------------------------------------------------------
touch "$KEYTAB_DIR/.kdc_ready"
echo ""
echo "============================================"
echo " KDC siap! Marker: $KEYTAB_DIR/.kdc_ready"
echo "============================================"

# Tetap jalan agar container tidak exit
wait $KDC_PID
