// Copyright 2026 The OpenSandbox Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objsync

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload    = "UNSIGNED-PAYLOAD"
	amzDateFormat      = "20060102T150405Z"
)

// S3Config addresses any S3-compatible service (AWS S3, MinIO, Aliyun OSS
// S3 endpoint, Ceph RGW).
type S3Config struct {
	// Endpoint like "http://minio.ns.svc:9000" or "https://oss-example.aliyuncs.com".
	// Empty means AWS S3 default endpoints.
	Endpoint string
	// Region defaults to us-east-1.
	Region string
	// Static credentials; empty credentials performs anonymous requests.
	AccessKey, SecretKey, SessionToken string
	// ForcePathStyle addresses objects as endpoint/bucket/key. Required for
	// most MinIO/Ceph deployments; AWS S3 and OSS default to virtual-host
	// style (bucket.endpoint/key).
	ForcePathStyle bool
}

// S3Store implements ObjectStore over the S3 REST API with SigV4 signing. It
// carries no bucket state: every call takes the bucket explicitly.
type S3Store struct {
	cfg    S3Config
	client *http.Client
}

// NewS3Store builds a store with sensible production defaults for the
// embedded HTTP client.
func NewS3Store(cfg S3Config) *S3Store {
	return &S3Store{cfg: cfg, client: &http.Client{Timeout: 0}} //nolint:gosec // per-request contexts bound the lifetime
}

// resolveHost computes request URL host and object path for bucket/key.
func (s *S3Store) resolveHost(bucket, key string) (string, string) {
	endpoint := strings.TrimSuffix(s.cfg.Endpoint, "/")
	if endpoint == "" {
		if s.cfg.Region == "" || s.cfg.Region == "us-east-1" {
			endpoint = "https://s3.amazonaws.com"
		} else {
			endpoint = "https://s3." + s.cfg.Region + ".amazonaws.com"
		}
	}
	if s.cfg.ForcePathStyle {
		return endpoint, "/" + bucket + "/" + key
	}
	// Virtual-host style (bucket.host/key) whenever the host is a DNS name;
	// IP literals cannot carry a bucket subdomain, so fall back to path style.
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" && !isIPv4Literal(u.Hostname()) && !strings.Contains(u.Hostname(), ":") {
		scheme := "https"
		if u.Scheme == "http" {
			scheme = "http"
		}
		return scheme + "://" + bucket + "." + u.Host, "/" + key
	}
	return endpoint, "/" + bucket + "/" + key
}

func isIPv4Literal(host string) bool {
	parts := strings.Split(host, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if n, err := strconv.Atoi(p); err != nil || n < 0 || n > 255 {
			return false
		}
	}
	return true
}

// request describes one signed S3 API call.
type request struct {
	method string
	bucket string
	key    string
	query  url.Values
	body   io.Reader
	size   int64
	// payloadSHA is the x-amz-content-sha256 value: a hex digest when known
	// (content addressing hands us file digests for free), the empty-payload
	// constant for bodiless calls, or UNSIGNED-PAYLOAD for streamed parts.
	payloadSHA string
}

func (s *S3Store) do(ctx context.Context, req request, out io.Writer) (http.Header, error) {
	hostURL, path := s.resolveHost(req.bucket, req.key)
	encodedPath := encodeS3Path(path)

	var body io.Reader
	if req.body != nil {
		body = req.body
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, hostURL+encodedPath, body)
	if err != nil {
		return nil, err
	}
	if req.query != nil {
		httpReq.URL.RawQuery = req.query.Encode()
	}
	if req.size > 0 {
		httpReq.ContentLength = req.size
	}

	payloadSHA := req.payloadSHA
	if payloadSHA == "" {
		payloadSHA = emptyPayloadSHA256
	}
	region := s.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	now := time.Now().UTC()
	if s.cfg.AccessKey != "" {
		s.sign(httpReq, region, now, payloadSHA)
	} else {
		httpReq.Header.Set("x-amz-date", now.Format(amzDateFormat))
		httpReq.Header.Set("x-amz-content-sha256", payloadSHA)
	}

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out != nil {
			if _, err := io.Copy(out, resp.Body); err != nil {
				return nil, fmt.Errorf("read response: %w", err)
			}
		}
		return resp.Header, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotFound
	default:
		msg := readS3Error(resp.Body)
		return nil, fmt.Errorf("s3 %s %s%s: HTTP %d: %s", req.method, req.bucket, path, resp.StatusCode, msg)
	}
}

// sign applies AWS Signature Version 4 to the request in place. It stamps the
// x-amz-date and x-amz-content-sha256 headers itself so signing is
// self-contained.
func (s *S3Store) sign(req *http.Request, region string, now time.Time, payloadSHA string) {
	amzDate := now.Format(amzDateFormat)
	dateStamp := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadSHA)
	if s.cfg.SessionToken != "" {
		req.Header.Set("x-amz-security-token", s.cfg.SessionToken)
	}

	// Canonical headers: host plus every signed-able header present on the
	// request (x-amz-*, content-type, content-md5, range).
	headers := map[string]string{"host": req.URL.Host}
	for name, values := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-type" || lower == "content-md5" || lower == "range" {
			headers[lower] = strings.TrimSpace(strings.Join(values, ","))
		}
	}
	signedNames := make([]string, 0, len(headers))
	for name := range headers {
		signedNames = append(signedNames, name)
	}
	sort.Strings(signedNames)

	var canonicalHeaders, signedHeaders strings.Builder
	for _, name := range signedNames {
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(headers[name])
		canonicalHeaders.WriteString("\n")
		signedHeaders.WriteString(name)
		signedHeaders.WriteString(";")
	}
	signedHeaderList := strings.TrimSuffix(signedHeaders.String(), ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		encodeS3Path(req.URL.Path),
		canonicalQueryString(req.URL.RawQuery),
		canonicalHeaders.String(),
		signedHeaderList,
		payloadSHA,
	}, "\n")

	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := hmacSHA256(
		hmacSHA256(
			hmacSHA256(
				hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), []byte(dateStamp)),
				[]byte(region)),
			[]byte("s3")),
		[]byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.cfg.AccessKey, scope, signedHeaderList, signature))
}

func canonicalQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return rawQuery
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

// encodeS3Path percent-encodes each path segment, keeping "/" separators,
// per the S3 SigV4 canonical URI rules (single encoding).
func encodeS3Path(path string) string {
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = uriEncode(seg)
	}
	return strings.Join(segments, "/")
}

// uriEncode is the AWS-specific component encoding: unreserved characters
// stay literal, everything else becomes %XX uppercase.
func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func readS3Error(r io.Reader) string {
	raw, err := io.ReadAll(io.LimitReader(r, 4096))
	if err != nil || len(raw) == 0 {
		return "unknown error"
	}
	var s3err struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.Unmarshal(raw, &s3err); err == nil && s3err.Code != "" {
		return s3err.Code + ": " + s3err.Message
	}
	return string(raw)
}

// ---- ObjectStore implementation ----

func (s *S3Store) Get(ctx context.Context, bucket, key string, w io.Writer) (int64, error) {
	buf := &countingWriter{w: w}
	if _, err := s.do(ctx, request{method: http.MethodGet, bucket: bucket, key: key}, buf); err != nil {
		return 0, err
	}
	return buf.n, nil
}

func (s *S3Store) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, sha256HexDigest string) error {
	payload := sha256HexDigest
	if payload == "" {
		payload = unsignedPayload
	}
	_, err := s.do(ctx, request{
		method: http.MethodPut, bucket: bucket, key: key,
		body: r, size: size, payloadSHA: payload,
	}, nil)
	return err
}

func (s *S3Store) Head(ctx context.Context, bucket, key string) (int64, bool, error) {
	hdr, err := s.do(ctx, request{method: http.MethodHead, bucket: bucket, key: key}, nil)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	size, _ := strconv.ParseInt(hdr.Get("Content-Length"), 10, 64)
	return size, true, nil
}

func (s *S3Store) Delete(ctx context.Context, bucket, key string) error {
	_, err := s.do(ctx, request{method: http.MethodDelete, bucket: bucket, key: key}, nil)
	return err
}

// ---- multipart upload ----

type mpuPart struct {
	number int
	etag   string
}

func (s *S3Store) PutMultipart(ctx context.Context, bucket, key string, r io.Reader, size, partSize int64) error {
	if partSize < 5<<20 {
		partSize = 5 << 20 // S3 minimum part size
	}
	uploadID, err := s.createMPU(ctx, bucket, key)
	if err != nil {
		return err
	}
	abort := func() {
		_, _ = s.do(ctx, request{
			method: http.MethodDelete, bucket: bucket, key: key,
			query: url.Values{"uploadId": {uploadID}},
		}, nil)
	}

	parts := make([]mpuPart, 0, size/partSize+1)
	partNumber := 0
	for remaining := size; remaining > 0; {
		partNumber++
		chunk := partSize
		if remaining < chunk {
			chunk = remaining
		}
		etag, err := s.uploadPart(ctx, bucket, key, uploadID, partNumber, io.LimitReader(r, chunk), chunk)
		if err != nil {
			abort()
			return err
		}
		parts = append(parts, mpuPart{number: partNumber, etag: etag})
		remaining -= chunk
	}
	if err := s.completeMPU(ctx, bucket, key, uploadID, parts); err != nil {
		abort()
		return err
	}
	return nil
}

func (s *S3Store) createMPU(ctx context.Context, bucket, key string) (string, error) {
	var out bytes.Buffer
	if _, err := s.do(ctx, request{
		method: http.MethodPost, bucket: bucket, key: key,
		query: url.Values{"uploads": {""}},
	}, &out); err != nil {
		return "", err
	}
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(out.Bytes(), &result); err != nil || result.UploadID == "" {
		return "", fmt.Errorf("decode initiate multipart result: %v", err)
	}
	return result.UploadID, nil
}

func (s *S3Store) uploadPart(ctx context.Context, bucket, key, uploadID string, number int, body io.Reader, size int64) (string, error) {
	hdr, err := s.do(ctx, request{
		method: http.MethodPut, bucket: bucket, key: key,
		query: url.Values{"partNumber": {strconv.Itoa(number)}, "uploadId": {uploadID}},
		body:  body, size: size,
		// Parts are streamed with unknown digest; HTTPS carries integrity.
		payloadSHA: unsignedPayload,
	}, nil)
	if err != nil {
		return "", err
	}
	etag := hdr.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("part %d missing ETag", number)
	}
	return etag, nil
}

func (s *S3Store) completeMPU(ctx context.Context, bucket, key, uploadID string, parts []mpuPart) error {
	type xmlPart struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	complete := struct {
		XMLName xml.Name  `xml:"CompleteMultipartUpload"`
		Parts   []xmlPart `xml:"Part"`
	}{}
	for _, p := range parts {
		complete.Parts = append(complete.Parts, xmlPart{PartNumber: p.number, ETag: p.etag})
	}
	body, err := xml.Marshal(complete)
	if err != nil {
		return err
	}
	// The CompleteMultipartUpload body is small; sign it with its digest.
	_, err = s.do(ctx, request{
		method: http.MethodPost, bucket: bucket, key: key,
		query:      url.Values{"uploadId": {uploadID}},
		body:       bytes.NewReader(body),
		size:       int64(len(body)),
		payloadSHA: sha256Hex(body),
	}, nil)
	return err
}

// ---- listing ----

func (s *S3Store) List(ctx context.Context, bucket, prefix, marker string, max int) ([]string, string, error) {
	query := url.Values{"list-type": {"2"}, "prefix": {prefix}}
	if marker != "" {
		query.Set("continuation-token", marker)
	}
	if max > 0 {
		query.Set("max-keys", strconv.Itoa(max))
	}
	var out bytes.Buffer
	if _, err := s.do(ctx, request{method: http.MethodGet, bucket: bucket, key: "", query: query}, &out); err != nil {
		return nil, "", err
	}
	var result struct {
		IsTruncated           bool   `xml:"IsTruncated"`
		NextContinuationToken string `xml:"NextContinuationToken"`
		Contents              []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(out.Bytes(), &result); err != nil {
		return nil, "", fmt.Errorf("decode list result: %w", err)
	}
	keys := make([]string, 0, len(result.Contents))
	for _, c := range result.Contents {
		keys = append(keys, c.Key)
	}
	return keys, result.NextContinuationToken, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
