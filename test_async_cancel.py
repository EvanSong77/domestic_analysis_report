#!/usr/bin/env python3
"""
快速测试异步协程取消功能
"""
import asyncio
import sys
from src.services.async_control import AsyncTaskController


async def mock_http_request(task_id: str, duration: int = 5):
    """模拟HTTP请求"""
    print(f"[{task_id}] 开始HTTP请求，预计耗时 {duration} 秒")
    try:
        for i in range(duration):
            await asyncio.sleep(1)
            print(f"[{task_id}] 请求进行中... {i+1}/{duration}")
        print(f"[{task_id}] HTTP请求完成")
        return {"status": "success", "task_id": task_id}
    except asyncio.CancelledError:
        print(f"[{task_id}] HTTP请求被取消")
        raise


async def test_basic_cancel():
    """测试基本取消功能"""
    print("=== 测试基本取消功能 ===")
    controller = AsyncTaskController()
    
    # 创建任务
    task = asyncio.create_task(mock_http_request("test_task_1", 10))
    controller.register_task("test_task_1", task)
    
    # 2秒后取消
    await asyncio.sleep(2)
    print("开始取消任务...")
    
    success = await controller.cancel_task("test_task_1", timeout=3)
    print(f"取消结果: {success}")
    
    assert success, "任务取消应该成功"
    print("✓ 基本取消功能测试通过")


async def test_multiple_tasks_cancel():
    """测试多个任务取消"""
    print("\n=== 测试多个任务取消 ===")
    controller = AsyncTaskController()
    
    # 创建多个任务
    tasks = []
    for i in range(3):
        task = asyncio.create_task(mock_http_request(f"multi_task_{i}", 8))
        controller.register_task(f"multi_task_{i}", task)
        tasks.append(task)
    
    # 检查任务状态
    print(f"当前运行任务: {controller.get_running_tasks()}")
    
    # 3秒后取消所有任务
    await asyncio.sleep(3)
    print("开始取消所有任务...")
    
    await controller.cancel_all_tasks(timeout=5)
    
    # 验证任务状态
    running_tasks = controller.get_running_tasks()
    print(f"取消后剩余任务: {running_tasks}")
    
    assert len(running_tasks) == 0, "所有任务应该都被取消"
    print("✓ 多个任务取消测试通过")


async def test_cancel_during_operation():
    """测试操作过程中的取消"""
    print("\n=== 测试操作过程中的取消 ===")
    controller = AsyncTaskController()
    
    async def long_operation(task_id: str):
        """长时间操作，包含多个步骤"""
        print(f"[{task_id}] 开始复杂操作")
        try:
            # 步骤1
            await asyncio.sleep(1)
            print(f"[{task_id}] 步骤1完成")
            
            # 步骤2（可能被取消）
            await asyncio.sleep(2)
            print(f"[{task_id}] 步骤2完成")
            
            # 步骤3
            await asyncio.sleep(1)
            print(f"[{task_id}] 步骤3完成")
            
            return "操作完成"
        except asyncio.CancelledError:
            print(f"[{task_id}] 操作被取消")
            raise
    
    # 启动操作
    task = asyncio.create_task(long_operation("complex_op"))
    controller.register_task("complex_op", task)
    
    # 在步骤2期间取消
    await asyncio.sleep(1.5)
    print("在操作过程中取消...")
    
    success = await controller.cancel_task("complex_op", timeout=2)
    print(f"取消结果: {success}")
    
    assert success, "操作过程中的取消应该成功"
    print("✓ 操作过程中取消测试通过")


async def test_controller_reuse():
    """测试控制器重用"""
    print("\n=== 测试控制器重用 ===")
    controller = AsyncTaskController()
    
    # 第一轮任务
    task1 = asyncio.create_task(mock_http_request("reuse_task_1", 3))
    controller.register_task("reuse_task_1", task1)
    await task1  # 等待完成
    
    # 第二轮任务
    task2 = asyncio.create_task(mock_http_request("reuse_task_2", 3))
    controller.register_task("reuse_task_2", task2)
    
    # 取消第二轮任务
    await asyncio.sleep(1)
    success = await controller.cancel_task("reuse_task_2", timeout=2)
    
    assert success, "控制器重用后取消应该成功"
    print("✓ 控制器重用测试通过")


async def main():
    """运行所有测试"""
    print("异步协程取消功能测试")
    print("=" * 50)
    
    try:
        await test_basic_cancel()
        await test_multiple_tasks_cancel()
        await test_cancel_during_operation()
        await test_controller_reuse()
        
        print("\n" + "=" * 50)
        print("所有测试通过！✓")
        return 0
        
    except AssertionError as e:
        print(f"\n测试失败: {e}")
        return 1
    except Exception as e:
        print(f"\n测试异常: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))