"""
多交易所批量提币程序
author: shuai
twitter: @ShuaiL0
"""

import ccxt
import json
import pandas as pd
import time
import random


def okx_withdraw(exchange, wallet_address, tag=None, currency=None, amount=None, chain=None):
    """
    在 OKX 交易所上执行提币操作。
    """
    exchange.timeout = 10000  # 设置接口超时时间为 10 秒

    # 获取所有币种列表
    try:
        currencies = exchange.fetchCurrencies()
    except Exception as e:
        print(f"[错误] 获取币种信息失败，跳过本条任务：{e}")
        return

    # 获取提币费用数据
    withdrawal_fee = None
    try:
        for key, value in currencies[currency]['networks'].items():
            if 'id' in value and value['info']['chain'] == chain:
                withdrawal_fee = value['fee']
                break
    except Exception as e:
        print(f"[错误] 提币链数据解析失败：{e}")
        return

    if not withdrawal_fee:
        print(f"[失败] 无法获取链 {chain} 的 {currency} 提币费用，跳过")
        return

    # 获取资金账户余额
    try:
        balance_data = exchange.privateGetAssetBalances()['data']
        balance = pd.DataFrame(balance_data)
        balance.set_index('ccy', inplace=True, drop=True)
        balance = balance.to_dict('index')
    except Exception as e:
        print(f"[错误] 获取余额失败：{e}")
        return

    if currency not in balance:
        print(f"[跳过] 没有 {currency} 的资金账户余额")
        return

    free_balance = float(balance[currency]['availBal'])
    print(f"可用 {currency} 余额：{free_balance}")

    if free_balance >= (amount + withdrawal_fee):
        print(f"正在将 {amount} {currency} 提现到钱包地址 {wallet_address} 提币链 {chain}")
        params = {
            'ccy': currency,
            'amt': amount,
            'dest': 4,
            'toAddr': wallet_address,
            'fee': withdrawal_fee,
            'chain': chain
        }
        if tag is not None:
            params['toAddr'] = f'{wallet_address}:{tag}'

        try:
            withdrawal = exchange.privatePostAssetWithdrawal(params)
            print("提现结果：", withdrawal['data'])
        except Exception as e:
            print(f"[错误] 提币请求失败：{e}")
            return

        # 查询提现状态
        time.sleep(5)
        try:
            status = exchange.privateGetAssetDepositWithdrawStatus(
                params={'wdId': withdrawal['data'][0]['wdId']}
            )
            print("提现状态：", status)
        except Exception as e:
            print(f"[警告] 提现状态查询失败：{e}")
    else:
        print(f"[跳过] 余额不足，{currency} 当前余额：{free_balance}，需要：{amount + withdrawal_fee}")
