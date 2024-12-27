import pandas as pd
from solana.transaction import Transaction
from solders.transaction import Transaction as SolderTransaction
import base64, time, random, json, ast, requests
from solders.keypair import Keypair
from solana.rpc.api import Client
from concurrent.futures import ThreadPoolExecutor


def send_modified_transaction(rpc_url: str, base64_data: str, private_key: list[int]) -> str:
    """
    发送修改后的 Solana 交易，并返回交易签名。

    :param rpc_url: Solana RPC 节点地址
    :param base64_data: Base64 编码的原始交易数据
    :param private_key: 以整数列表表示的私钥
    :return: 交易签名 (str)
    """
    client = Client(rpc_url)

    # 获取最新的区块哈希，最多重试 5 次
    def get_latest_blockhash_with_retry(client, retries=5, delay=0.5):
        for attempt in range(1, retries + 1):
            try:
                return client.get_latest_blockhash().value.blockhash
            except Exception as e:
                print(f"获取区块哈希失败，错误: {e}")
                if attempt < retries:
                    print(f"正在进行第 {attempt} 次重试...")
                    time.sleep(delay)
                else:
                    print("获取区块哈希失败，已达到最大重试次数")
                    return None

    blockhash = get_latest_blockhash_with_retry(client)
    if not blockhash:
        raise RuntimeError("无法获取最新区块哈希，交易中止")

    # 创建 Keypair
    keypair = Keypair.from_bytes(bytes(private_key))

    # 解码 Base64 数据并反序列化为 Transaction 对象
    try:
        byte_data = base64.b64decode(base64_data)
        tx_json = Transaction.deserialize(byte_data).to_solders().to_json()
        tx = json.loads(tx_json)
    except Exception as e:
        raise ValueError(f"交易数据反序列化失败: {e}")

    # 修改交易的最后一个字节数据
    try:
        random_number = random.randint(0, 255)
        tx['message']['instructions'][1]['data'][-1] = random_number
        modified_tx_json = json.dumps(tx)
        tx = Transaction.from_solders(SolderTransaction.from_json(modified_tx_json))
    except Exception as e:
        raise ValueError(f"修改交易数据失败: {e}")

    # 更新区块哈希和重新签名
    tx.recent_blockhash = blockhash
    tx.sign(keypair)

    # 序列化并发送交易，返回交易签名
    try:
        tx_signature = client.send_raw_transaction(tx.serialize(False)).value
        print(f"交易成功发送，交易签名: {tx_signature}")
        return tx_signature
    except Exception as e:
        raise RuntimeError(f"交易发送失败: {e}")


def check_transaction_status(client: Client, tx_signature: str) -> bool:
    """
    检查交易状态，基于 client.get_transaction 的结果。

    :param client: Solana RPC 客户端。
    :param tx_signature: 交易签名。
    :return: 如果交易成功返回 True，否则返回 False。
    """
    try:
        res = client.get_transaction(tx_signature)  # 获取交易数据

        # 检查是否存在交易数据
        if res.value is None:
            print(f"交易未找到或尚未确认，交易签名: {tx_signature}")
            return False

        # 解构交易数据
        transaction_data = res.value

        # 检查元数据是否存在
        if transaction_data.transaction.meta is None:
            print(f"交易的元数据不存在，交易可能未完成，交易签名: {tx_signature}")
            return False

        # 检查是否有错误
        if transaction_data.transaction.meta.err is None:
            print(f"交易成功，交易签名: {tx_signature}")
            return True
        else:
            print(f"交易失败或存在错误，交易签名: {tx_signature}, 错误详情: {transaction_data.transaction.meta.err}")
            return False

    except Exception as e:
        print(f"检测交易状态时发生错误: {e}")
        return False


def read_data_from_csv(file_path: str) -> list[dict]:
    """
    从 CSV 文件中读取私钥、交易数据和代理设置。

    :param file_path: CSV 文件路径。
    :return: 每行包含私钥、交易数据、循环次数和代理设置的字典列表。
    """
    try:
        df = pd.read_csv(file_path)  # 读取 CSV 文件
        data = []
        for _, row in df.iterrows():
            private_key = ast.literal_eval(row["private_key"])  # 将字符串形式的私钥转为列表
            transaction_data = row["data"]  # Base64 编码的交易数据
            repeat_count = int(row["repeat_count"])  # 读取循环次数
            data.append({"private_key": private_key, "data": transaction_data, "repeat_count": repeat_count})
        return data
    except Exception as e:
        raise RuntimeError(f"读取 CSV 文件失败: {e}")


def process_wallet(rpc, private_key, data, repeat_count):
    """
    处理单个钱包的交易逻辑。
    """
    client = Client(rpc)
    keypair = Keypair.from_bytes(bytes(private_key))  # 创建 Keypair
    wallet_address = keypair.pubkey()  # 获取钱包地址

    print(f"开始处理钱包 {wallet_address}，循环交易 {repeat_count} 次...")
    for iteration in range(repeat_count):  # 循环发送交易
        try:
            print(f"开始第 {iteration + 1}/{repeat_count} 次交易...")
            tx_signature = send_modified_transaction(rpc, data, private_key)
            print(f"第 {iteration + 1}/{repeat_count} 次交易的交易签名: {tx_signature}")

            # 检测上一笔交易是否成功
            while True:
                if check_transaction_status(client, tx_signature):
                    # 随机等待 5-10 秒
                    sleep_time = random.uniform(2, 5)
                    print(f"随机等待 {sleep_time:.2f} 秒...")
                    time.sleep(sleep_time)
                    break
                else:
                    print("交易尚未确认，等待 5 秒后重试...")
                    time.sleep(5)  # 等待 5 秒后重试

        except Exception as e:
            print(f"第 {iteration + 1}/{repeat_count} 次交易发生错误: {e}")


# 主流程
if __name__ == "__main__":
    rpc = "https://eclipse.lgns.net"  # 替换为你的 RPC URL
    csv_file = "transactions.csv"  # CSV 文件路径

    # 从 CSV 文件中读取私钥和交易数据
    try:
        transaction_data_list = read_data_from_csv(csv_file)
    except Exception as e:
        print(f"读取 CSV 文件失败: {e}")
        exit(1)

    # 并发处理每个钱包
    with ThreadPoolExecutor() as executor:
        futures = []
        for transaction_data in transaction_data_list:
            private_key = transaction_data["private_key"]
            data = transaction_data["data"]
            repeat_count = transaction_data["repeat_count"]

            # 提交任务到线程池
            futures.append(executor.submit(process_wallet, rpc, private_key, data, repeat_count))

        # 等待所有任务完成
        for future in futures:
            future.result()
