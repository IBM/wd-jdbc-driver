package com.ibm.wd.connector.jdbc.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class ReadOnlyNClob implements NClob {

    private String data;

    public ReadOnlyNClob(String data) {
        this.data = data;
    }

    @Override
    public long length() throws SQLException {
        return data.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return data.substring((int)pos, (int)pos + length);
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(data.substring((int)pos, (int)(pos + length)));
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(data);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.ISO_8859_1));
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        return data.indexOf(searchstr, (int)start);
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return position(searchstr.getSubString(0, (int)searchstr.length()), start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        this.data = "";
    }

}
