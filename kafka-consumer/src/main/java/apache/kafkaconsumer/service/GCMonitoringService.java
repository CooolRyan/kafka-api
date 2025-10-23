package apache.kafkaconsumer.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// @Service  // GC 모니터링 완전 비활성화
@Slf4j
public class GCMonitoringService {

    private final MeterRegistry meterRegistry;
    private final MemoryMXBean memoryBean;
    private final GarbageCollectorMXBean youngGCBean;
    private final GarbageCollectorMXBean oldGCBean;
    
    // GC 모니터링을 위한 변수들
    private long lastYoungGCCount = 0;
    private long lastYoungGCTime = 0;
    private long lastOldGCCount = 0;
    private long lastOldGCTime = 0;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public GCMonitoringService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        
        // G1GC의 경우 다른 이름을 가질 수 있음
        this.youngGCBean = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .filter(gc -> gc.getName().contains("Young") || gc.getName().contains("G1 Young"))
                .findFirst()
                .orElse(ManagementFactory.getGarbageCollectorMXBeans().get(0));
        
        this.oldGCBean = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .filter(gc -> gc.getName().contains("Old") || gc.getName().contains("G1 Old"))
                .findFirst()
                .orElse(ManagementFactory.getGarbageCollectorMXBeans().get(0));
    }

    @PostConstruct
    public void initGCMetrics() {
        // 메모리 사용량 메트릭
        Gauge.builder("jvm.memory.used.heap", memoryBean, bean -> {
                    MemoryUsage heapUsage = bean.getHeapMemoryUsage();
                    return heapUsage.getUsed();
                })
                .description("힙 메모리 사용량 (bytes)")
                .register(meterRegistry);

        Gauge.builder("jvm.memory.max.heap", memoryBean, bean -> {
                    MemoryUsage heapUsage = bean.getHeapMemoryUsage();
                    return heapUsage.getMax();
                })
                .description("힙 메모리 최대 크기 (bytes)")
                .register(meterRegistry);

        // GC 횟수 메트릭
        Gauge.builder("jvm.gc.collections.young", youngGCBean, GarbageCollectorMXBean::getCollectionCount)
                .description("Young GC 횟수")
                .register(meterRegistry);

        Gauge.builder("jvm.gc.collections.old", oldGCBean, GarbageCollectorMXBean::getCollectionCount)
                .description("Old GC 횟수")
                .register(meterRegistry);

        Gauge.builder("jvm.gc.time.young", youngGCBean, GarbageCollectorMXBean::getCollectionTime)
                .description("Young GC 시간 (ms)")
                .register(meterRegistry);

        Gauge.builder("jvm.gc.time.old", oldGCBean, GarbageCollectorMXBean::getCollectionTime)
                .description("Old GC 시간 (ms)")
                .register(meterRegistry);

        log.info("GC 모니터링 메트릭이 초기화되었습니다.");
        log.info("Young GC: {}, Old GC: {}", youngGCBean.getName(), oldGCBean.getName());
        
        // 초기값 설정
        lastYoungGCCount = youngGCBean.getCollectionCount();
        lastYoungGCTime = youngGCBean.getCollectionTime();
        lastOldGCCount = oldGCBean.getCollectionCount();
        lastOldGCTime = oldGCBean.getCollectionTime();
        
        // GC 이벤트 실시간 모니터링 시작 (5초마다 체크 - 배치 처리 영향 최소화)
        startRealTimeGCMonitoring();
    }
    
    /**
     * 실시간 GC 모니터링 시작
     */
    private void startRealTimeGCMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkGCChanges();
            } catch (Exception e) {
                log.error("GC 모니터링 중 오류 발생: {}", e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
        
        log.info("실시간 GC 모니터링이 시작되었습니다. (5초 간격)");
    }
    
    /**
     * GC 변화 감지 및 로그 출력
     */
    private void checkGCChanges() {
        long currentYoungGCCount = youngGCBean.getCollectionCount();
        long currentYoungGCTime = youngGCBean.getCollectionTime();
        long currentOldGCCount = oldGCBean.getCollectionCount();
        long currentOldGCTime = oldGCBean.getCollectionTime();
        
        // Young GC 발생 감지
        if (currentYoungGCCount > lastYoungGCCount) {
            long gcCount = currentYoungGCCount - lastYoungGCCount;
            long gcTime = currentYoungGCTime - lastYoungGCTime;
            long avgTime = gcTime / gcCount;
            
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            long heapUsedMB = heapUsage.getUsed() / 1024 / 1024;
            long heapMaxMB = heapUsage.getMax() / 1024 / 1024;
            double heapUsagePercent = (double) heapUsedMB / heapMaxMB * 100;
            
            log.warn("🟡 YOUNG GC 발생! {}회, 총 {}ms, 평균 {}ms/회, 힙사용률: {:.1f}% ({}MB/{}MB)", 
                    gcCount, gcTime, avgTime, heapUsagePercent, heapUsedMB, heapMaxMB);
            
            lastYoungGCCount = currentYoungGCCount;
            lastYoungGCTime = currentYoungGCTime;
        }
        
        // Old GC 발생 감지
        if (currentOldGCCount > lastOldGCCount) {
            long gcCount = currentOldGCCount - lastOldGCCount;
            long gcTime = currentOldGCTime - lastOldGCTime;
            long avgTime = gcTime / gcCount;
            
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            long heapUsedMB = heapUsage.getUsed() / 1024 / 1024;
            long heapMaxMB = heapUsage.getMax() / 1024 / 1024;
            double heapUsagePercent = (double) heapUsedMB / heapMaxMB * 100;
            
            log.error("🔴 OLD GC 발생! {}회, 총 {}ms, 평균 {}ms/회, 힙사용률: {:.1f}% ({}MB/{}MB)", 
                    gcCount, gcTime, avgTime, heapUsagePercent, heapUsedMB, heapMaxMB);
            
            lastOldGCCount = currentOldGCCount;
            lastOldGCTime = currentOldGCTime;
        }
    }

    /**
     * 현재 메모리 상태를 로그로 출력
     */
    public void logMemoryStatus() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
        
        long heapUsed = heapUsage.getUsed() / 1024 / 1024; // MB
        long heapMax = heapUsage.getMax() / 1024 / 1024; // MB
        long nonHeapUsed = nonHeapUsage.getUsed() / 1024 / 1024; // MB
        
        double heapUsagePercent = (double) heapUsed / heapMax * 100;
        
        log.info("=== 메모리 상태 ===");
        log.info("힙 메모리: {}MB / {}MB ({}%)", heapUsed, heapMax, String.format("%.2f", heapUsagePercent));
        log.info("비힙 메모리: {}MB", nonHeapUsed);
        log.info("Young GC: {}회, {}ms", youngGCBean.getCollectionCount(), youngGCBean.getCollectionTime());
        log.info("Old GC: {}회, {}ms", oldGCBean.getCollectionCount(), oldGCBean.getCollectionTime());
    }
}
