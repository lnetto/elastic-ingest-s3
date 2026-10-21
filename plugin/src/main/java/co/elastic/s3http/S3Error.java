package co.elastic.s3http;

/** An S3 error response: HTTP status + the XML {@code <Error>} body S3 clients parse. */
record S3Error(int status, String code, String message) {

    String xml(String resource, String requestId) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + esc(code) + "</Code><Message>" + esc(message)
            + "</Message><Resource>" + esc(resource) + "</Resource><RequestId>" + esc(requestId) + "</RequestId></Error>";
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
