/*
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import org.junit.jupiter.api.Test;

import java.net.SocketException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class SqlServerStreamingChangeEventSourceTest {

    @Test
    void shouldFailFastWhenCdcLsnRangeIsInvalid() {
        SQLException sqlException =
                new SQLException("LSN range is no longer available", "S0001", 313);

        IllegalStateException exception =
                SqlServerStreamingChangeEventSource.buildInvalidCdcLsnRangeException(
                        "inventory",
                        TxLogPosition.valueOf(Lsn.valueOf("00000027:00000758:0005")),
                        Arrays.asList("dbo_products", "dbo_orders"),
                        sqlException);

        assertThat(exception)
                .hasMessageContaining("database 'inventory'")
                .hasMessageContaining("error 313")
                .hasMessageContaining("saved offset is no longer valid")
                .hasMessageContaining("new snapshot")
                .hasMessageContaining("minimum available LSN")
                .hasMessageContaining("sys.fn_cdc_get_min_lsn")
                .hasMessageContaining("dbo_products, dbo_orders")
                .hasCause(sqlException);
    }

    @Test
    void shouldRetryConnectionResetWithoutFailingTheOffset() {
        SQLException connectionReset =
                new SQLException(
                        "Connection reset", "08S01", new SocketException("Connection reset"));

        assertThat(
                        SqlServerStreamingChangeEventSource.isRecoverableConnectionFailure(
                                connectionReset))
                .isTrue();
    }

    @Test
    void shouldRetryWhenTheDriverReportsTheConnectionClosed() {
        assertThat(
                        SqlServerStreamingChangeEventSource.isRecoverableConnectionFailure(
                                new SQLException("该连接已关闭。")))
                .isTrue();
        assertThat(
                        SqlServerStreamingChangeEventSource.isRecoverableConnectionFailure(
                                new SQLException("The connection is closed.")))
                .isTrue();
    }

    @Test
    void shouldNotRetryWhenTheCdcLsnWindowWasPurged() {
        SQLException sqlException =
                new SQLException("LSN range is no longer available", "S0001", 313);
        IllegalStateException purgedWindow =
                SqlServerStreamingChangeEventSource.buildInvalidCdcLsnRangeException(
                        "inventory",
                        TxLogPosition.valueOf(Lsn.valueOf("00000027:00000758:0005")),
                        Arrays.asList("dbo_products"),
                        sqlException);

        assertThat(SqlServerStreamingChangeEventSource.isRecoverableConnectionFailure(sqlException))
                .isFalse();
        assertThat(SqlServerStreamingChangeEventSource.isRecoverableConnectionFailure(purgedWindow))
                .isFalse();
    }
}
