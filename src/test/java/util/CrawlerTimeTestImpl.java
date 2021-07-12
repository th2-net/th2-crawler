package util;

import com.exactpro.th2.crawler.util.CrawlerTime;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class CrawlerTimeTestImpl implements CrawlerTime {
    private Instant instant = Instant.parse("2021-07-11T18:00:00.00Z");
    private long count = -1;

    @Override
    public Instant now() {
        count++;
        return instant.plus(10 * count, ChronoUnit.SECONDS);
    }
}
