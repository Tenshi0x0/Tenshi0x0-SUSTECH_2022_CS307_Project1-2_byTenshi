package com.pgsqlproject2.pgproj2;


import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringsApplicationTests {

    @Test
    public void contextLoads() {
    }

    /**
     * 引入 ContiPerf 进行性能测试
     * 激活性能测试，否则@PerfTest 无法生效
     */
    @Rule
    public ContiPerfRule contiPerfRule = new ContiPerfRule();

    /**
     * 100 个线程 执行 100 次
     * invocations:调用次数，执行次数与线程无关
     * threads:线程
     */
    @Test
    @PerfTest(invocations = 100, threads = 100)
    public void test() {


    }

}
