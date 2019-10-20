from nonebot import on_command, CommandSession
import re


import app.coolq.coolq_client as coolqClient


kOrderPattern = re.compile(r'^\s*([\d\.]+)元?\s*(卖|买)\s*(\d+)股(\d+)')
kIntPattern = re.compile(r'\d+')
kStrategyPattern = re.compile(r'^\s*(策略)?(\w+)\s*股票(\d+)')


async def send_command(message: str):
    client = coolqClient.get_coolq_client()
    if client is None:
        return
    await client.send_command(message)


@on_command('help', aliases=('帮助', '用法'))
async def help(session: CommandSession):
    help_doc = ['帮助: 列出此帮助文档',
                '投资: 用于获取用户当前投资组合',
                '现金: 获取用户当前现金数',
                '股票: 获取可交易的股票id，用于下单',
                '订单: 获取当前用户所有已下单未成交的订单',
                '下单: 用于下单购买/卖出某股票',
                '撤单: 用于撤销未成交订单',
                '订阅行情: 用于定于股票市场行情',
                '退订行情: 用于退订股票市场行情',
                '订阅深度: 用于定于股票市场行情深度',
                '退订深度: 用于退订股票市场行情深度',
                '策略: 列出当前所有可用策略',
                '策略状况: 列出当前正在执行的策略的情况',
                '执行策略: 开始执行指定策略',
                '停止策略: 停止正在执行的策略']
    await session.send('\n'.join(help_doc))


@on_command('portfolio', aliases=('投资', '投资组合', '组合'))
async def portfolio(session: CommandSession):
    await send_command('portfolio')


@on_command('cash', aliases=('现金', '资产'))
async def cash(session: CommandSession):
    await send_command('cash')


@on_command('contract', aliases=('股票',))
async def contract(session: CommandSession):
    await send_command('contract')


@on_command('orders', aliases=('订单',))
async def orders(session: CommandSession):
    await send_command('orders')


def parse_order_command(command: str):
    m = kOrderPattern.match(command)
    if m:
        command = []
        command.append(m.group(4))
        if m.group(2) == '卖':
            command.append('sell')
        else:
            command.append('buy')
        command.append(m.group(3))
        command.append(m.group(1))
        return command
    return None


@on_command('order', aliases=('下单',))
async def order(session: CommandSession):
    command = session.get('command', prompt='请以如下格式下单\n'
                          + '16.5元买/卖1000股0')
    commands = parse_order_command(command)
    if commands is None:
        await session.send('参数错误')
    else:
        cmd = ' '.join(['order', *commands])
        await send_command(cmd)


@order.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['command'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('命令不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('cancel_order', aliases=('撤销', '撤单'))
async def cancel_order(session: CommandSession):
    order_id = session.get('order_id', prompt='请输入需要撤单的订单号')
    if not kIntPattern.match(order_id):
        session.send('无效订单号')
        return
    await send_command(f'cancel_order {order_id}')


@cancel_order.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['order_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('订单号不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('subscribe_market', aliases=('订阅行情', '订阅市场'))
async def subscribe_market(session: CommandSession):
    contract_id = session.get('contract_id', prompt='请输入股票id')
    if not kIntPattern.match(contract_id):
        session.send('无效股票id')
        return
    await send_command(f'subscribe_market {contract_id}')


@subscribe_market.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['contract_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('股票id不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('unsubscribe_market', aliases=('退订行情', '退订市场'))
async def unsubscribe_market(session: CommandSession):
    contract_id = session.get('contract_id', prompt='请输入股票id')
    if not kIntPattern.match(contract_id):
        session.send('无效股票id')
        return
    await send_command(f'unsubscribe_market {contract_id}')


@unsubscribe_market.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['contract_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('股票id不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('subscribe_market_depth', aliases=('订阅深度',))
async def subscribe_market_depth(session: CommandSession):
    contract_id = session.get('contract_id', prompt='请输入股票id')
    if not kIntPattern.match(contract_id):
        session.send('无效股票id')
        return
    await send_command(f'subscribe_market_depth {contract_id}')


@subscribe_market_depth.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['contract_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('股票id不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('unsubscribe_market_depth', aliases=('退订深度',))
async def unsubscribe_market_depth(session: CommandSession):
    contract_id = session.get('contract_id', prompt='请输入股票id')
    if not kIntPattern.match(contract_id):
        session.send('无效股票id')
        return
    await send_command(f'unsubscribe_market_depth {contract_id}')


@unsubscribe_market_depth.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['contract_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('股票id不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('list_strategies', aliases=('策略',))
async def list_strategies(session: CommandSession):
    await send_command('list_strategies')


@on_command('list_running_strategies', aliases=('策略状况',))
async def list_running_strategies(session: CommandSession):
    await send_command('list_running_strategies')


@on_command('start_strategy', aliases=('执行策略',))
async def start_strategy(session: CommandSession):
    command = session.get('command', prompt='请输入策略号和股票id，如:\n策略medium股票0')
    m = kStrategyPattern.match(command)
    if not m:
        session.send('参数错误')
    await send_command(f'start_strategy {m.group(3)} {m.group(2)}')


@start_strategy.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['command'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('参数不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg


@on_command('stop_strategy', aliases=('停止策略',))
async def stop_strategy(session: CommandSession):
    strategy_id = session.get('strategy_id', prompt='请输入需要停止的策略号')
    if not kIntPattern.match(strategy_id):
        session.send('无效的策略号')
        return
    await send_command(f'stop_strategy {strategy_id}')


@stop_strategy.args_parser
async def _(session: CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.is_first_run:
        if stripped_arg:
            session.state['strategy_id'] = stripped_arg
        return

    if not stripped_arg:
        session.pause('策略号不能为空，请重新输入')

    session.state[session.current_key] = stripped_arg
