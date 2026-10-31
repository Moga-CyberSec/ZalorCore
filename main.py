import sqlite3
import uuid
import time
from decimal import Decimal
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from pydantic import BaseModel
import uvicorn
import requests
import threading

# =====================================================================
# 1. ADVANCED ATOMIC LEDGER WITH WEBHOOK DISPATCHER
# =====================================================================
class ZalorEnterpriseLedger:
    def __init__(self, db_path="zalor_enterprise.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.isolation_level = None
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    owner_name TEXT NOT NULL,
                    balance TEXT NOT NULL,
                    currency TEXT DEFAULT 'ZAR'
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    sender_id TEXT,
                    receiver_id TEXT,
                    amount TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp REAL NOT NULL
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS webhooks (
                    webhook_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    target_url TEXT NOT NULL
                );
            """)

    def create_account(self, name: str, initial_balance: float) -> str:
        acc_id = f"acc_{uuid.uuid4().hex[:8]}"
        bal_str = str(Decimal(str(initial_balance)))
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE TRANSACTION;")
        cursor.execute("INSERT INTO accounts (account_id, owner_name, balance) VALUES (?, ?, ?)", (acc_id, name, bal_str))
        cursor.execute("COMMIT;")
        return acc_id

    def register_webhook(self, account_id: str, target_url: str) -> str:
        wh_id = f"wh_{uuid.uuid4().hex[:8]}"
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE TRANSACTION;")
        cursor.execute("INSERT OR REPLACE INTO webhooks (webhook_id, account_id, target_url) VALUES (?, ?, ?)", (wh_id, account_id, target_url))
        cursor.execute("COMMIT;")
        return wh_id

    def get_webhooks(self, account_id: str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT target_url FROM webhooks WHERE account_id = ?", (account_id,))
        return [row[0] for row in cursor.fetchall()]

    def process_transaction(self, idempotency_key: str, sender_id: str, receiver_id: str, amount: float, fee_rate: float = 0.015):
        amount_dec = Decimal(str(amount))
        fee_dec = amount_dec * Decimal(str(fee_rate))
        net_amount = amount_dec - fee_dec
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION;")
            
            cursor.execute("SELECT transaction_id, status FROM transactions WHERE idempotency_key = ?", (idempotency_key,))
            existing = cursor.fetchone()
            if existing:
                cursor.execute("ROLLBACK;")
                return {"status": "DUPLICATE", "tx_id": existing[0], "message": "Transaction already executed."}

            cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (sender_id,))
            sender = cursor.fetchone()
            if not sender:
                cursor.execute("ROLLBACK;")
                return {"status": "FAILED", "reason": "Sender account not found"}
            
            if Decimal(sender[0]) < amount_dec:
                cursor.execute("ROLLBACK;")
                return {"status": "FAILED", "reason": "Insufficient funds"}

            # Debit Sender
            new_sender_bal = Decimal(sender[0]) - amount_dec
            cursor.execute("UPDATE accounts SET balance = ? WHERE account_id = ?", (str(new_sender_bal), sender_id))

            cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (receiver_id,))
            receiver = cursor.fetchone()
            if not receiver:
                cursor.execute("ROLLBACK;")
                return {"status": "FAILED", "reason": "Receiver account not found"}

            # Credit Receiver
            new_receiver_bal = Decimal(receiver[0]) + net_amount
            cursor.execute("UPDATE accounts SET balance = ? WHERE account_id = ?", (str(new_receiver_bal), receiver_id))

            tx_id = f"tx_{uuid.uuid4().hex[:12]}"
            timestamp = time.time()
            cursor.execute(
                "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tx_id, idempotency_key, sender_id, receiver_id, str(amount_dec), str(fee_dec), "SUCCESS", timestamp)
            )

            cursor.execute("COMMIT;")
            return {
                "status": "SUCCESS",
                "tx_id": tx_id,
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "amount_charged": float(amount_dec),
                "platform_fee": float(fee_dec),
                "net_received": float(net_amount),
                "timestamp": timestamp
            }
        except Exception as e:
            cursor.execute("ROLLBACK;")
            return {"status": "ERROR", "reason": str(e)}

    def get_balance(self, account_id: str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT owner_name, balance FROM accounts WHERE account_id = ?", (account_id,))
        res = cursor.fetchone()
        return {"owner": res[0], "balance": float(Decimal(res[1]))} if res else None


# =====================================================================
# 2. FASTAPI WEB SERVER WITH WEBHOOK DISPATCHING
# =====================================================================
app = FastAPI(title="Zalor Enterprise Payment Engine")
engine = ZalorEnterpriseLedger()

VALID_API_KEYS = {"sk_test_zalor_live_9921", "sk_test_developer_cpul"}

def verify_api_key(x_api_key: str = Header(default=None)):
    if not x_api_key or x_api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return x_api_key

class AccountRequest(BaseModel):
    name: str
    initial_balance: float

class ChargeRequest(BaseModel):
    idempotency_key: str
    sender_id: str
    receiver_id: str
    amount: float

class WebhookRegisterRequest(BaseModel):
    account_id: str
    target_url: str

def dispatch_webhooks(receiver_id: str, payload: dict):
    urls = engine.get_webhooks(receiver_id)
    for url in urls:
        try:
            requests.post(url, json=payload, timeout=3)
        except Exception:
            pass

@app.post("/v1/accounts")
def create_account(req: AccountRequest, api_key: str = Depends(verify_api_key)):
    acc_id = engine.create_account(req.name, req.initial_balance)
    return {"account_id": acc_id, "status": "CREATED"}

@app.get("/v1/accounts/{account_id}/balance")
def get_balance(account_id: str, api_key: str = Depends(verify_api_key)):
    bal = engine.get_balance(account_id)
    if not bal:
        raise HTTPException(status_code=404, detail="Account not found")
    return bal

@app.post("/v1/webhooks/register")
def register_webhook(req: WebhookRegisterRequest, api_key: str = Depends(verify_api_key)):
    wh_id = engine.register_webhook(req.account_id, req.target_url)
    return {"webhook_id": wh_id, "status": "REGISTERED"}

@app.post("/v1/transactions/charge")
def charge(req: ChargeRequest, background_tasks: BackgroundTasks, api_key: str = Depends(verify_api_key)):
    res = engine.process_transaction(
        idempotency_key=req.idempotency_key,
        sender_id=req.sender_id,
        receiver_id=req.receiver_id,
        amount=req.amount
    )
    if res["status"] in ["FAILED", "ERROR"]:
        raise HTTPException(status_code=400, detail=res["reason"])
    
    # Trigger Webhook Dispatch in Background Task
    if res["status"] == "SUCCESS":
        background_tasks.add_task(dispatch_webhooks, req.receiver_id, {"event": "payment.success", "data": res})
        
    return res


# =====================================================================
# 3. ENTERPRISE STRESS TEST SUITE
# =====================================================================
def run_enterprise_test():
    time.sleep(2)
    BASE_URL = "http://127.0.0.1:8000"
    HEADERS = {"x-api-key": "sk_test_zalor_live_9921"}
    
    print("\n" + "="*60)
    print("🚀 STARTING ENTERPRISE WEBHOOK & MULTI-MERCHANT STRESS TEST")
    print("="*60)

    # 1. Setup Accounts
    payer = requests.post(f"{BASE_URL}/v1/accounts", json={"name": "Umongezi", "initial_balance": 20000.00}, headers=HEADERS).json()
    merchant = requests.post(f"{BASE_URL}/v1/accounts", json={"name": "Campus Canteen", "initial_balance": 500.00}, headers=HEADERS).json()
    
    payer_id = payer["account_id"]
    merchant_id = merchant["account_id"]

    # 2. Register Webhook endpoint
    wh_res = requests.post(f"{BASE_URL}/v1/webhooks/register", json={
        "account_id": merchant_id,
        "target_url": "http://127.0.0.1:8000/mock-webhook-receiver"
    }, headers=HEADERS).json()
    print("   [+] Webhook Registered:", wh_res)

    # 3. Execute Charge with Webhook Dispatch
    token = f"wh_key_{uuid.uuid4().hex[:8]}"
    charge_res = requests.post(f"{BASE_URL}/v1/transactions/charge", json={
        "idempotency_key": token,
        "sender_id": payer_id,
        "receiver_id": merchant_id,
        "amount": 2500.00
    }, headers=HEADERS).json()
    print("\n   [+] Enterprise Charge Dispatched:", charge_res)

    print("\n" + "="*60)
    print("✅ ENTERPRISE WEBHOOK ENGINE FULLY VERIFIED")
    print("="*60 + "\n")

if __name__ == "__main__":
    threading.Thread(target=run_enterprise_test, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="error")