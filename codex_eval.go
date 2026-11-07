package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// ---------------------------------------------------------------------------
// Codex turn construction (ported from cpa-plugin-codex-candy-eval). This is a
// first request of a new codex-tui thread on Responses Lite, captured in
// data/codex_context.json.
// ---------------------------------------------------------------------------

const (
	codexResponsesUserAgent = "codex-tui/0.163.0 (Debian 13.0.0; x86_64) xterm-ghostty (codex-tui; 0.163.0)"
	codexEnvironment        = `<environment_context>
  <cwd>/home/user/workspace</cwd>
  <shell>bash</shell>
  <current_date>%s</current_date>
  <timezone>Etc/UTC</timezone>
  <filesystem><workspace_roots><root>/home/user/workspace</root></workspace_roots><permission_profile type="managed"><file_system type="restricted"><entry access="read"><special>:root</special></entry><entry access="write"><path>/home/user/workspace</path></entry><entry access="write"><special>:slash_tmp</special></entry><entry access="write"><special>:tmpdir</special></entry><entry access="read"><path>/home/user/workspace/.git</path></entry><entry access="read"><path>/home/user/workspace/.agents</path></entry><entry access="read"><path>/home/user/workspace/.codex</path></entry><entry access="read"><path>/home/user/workspace/.aws</path></entry></file_system></permission_profile></filesystem>
</environment_context>`
)

// codexText is one text part of a context message, labeled with the kind Codex reports for it.
type codexText struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

//go:embed data/codex_context.json
var codexContextJSON []byte

var codexContext = mustDecodeEval[struct {
	Tools            json.RawMessage `json:"tools"`
	BaseInstructions string          `json:"base_instructions"`
	Messages         []struct {
		Role    string      `json:"role"`
		Content []codexText `json:"content"`
	} `json:"messages"`
}](codexContextJSON)

// codexTools is the tools list as Codex sends it; the tools item ID hashes these exact bytes.
var codexTools = func() []byte {
	var tools bytes.Buffer
	if err := json.Compact(&tools, codexContext.Tools); err != nil {
		panic(err)
	}
	return tools.Bytes()
}()

type codexItem struct {
	Type     string             `json:"type"`
	ID       string             `json:"id"`
	Role     string             `json:"role"`
	Tools    json.RawMessage    `json:"tools,omitempty"`
	Content  []codexContent     `json:"content,omitempty"`
	Metadata *codexItemMetadata `json:"internal_chat_message_metadata_passthrough,omitempty"`
}

type codexContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type codexItemMetadata struct {
	TurnID           string   `json:"turn_id,omitempty"`
	CreateTime       float64  `json:"create_time,omitempty"`
	ContentItemKinds []string `json:"content_item_kinds"`
}

type codexReasoning struct {
	Effort  string `json:"effort,omitempty"`
	Context string `json:"context"`
}

type codexClientMetadata struct {
	ThreadID       string `json:"thread_id"`
	WindowID       string `json:"x-codex-window-id"`
	TurnID         string `json:"turn_id"`
	RootTurnID     string `json:"root_turn_id"`
	TurnMetadata   string `json:"x-codex-turn-metadata"`
	InstallationID string `json:"x-codex-installation-id"`
	SessionID      string `json:"session_id"`
}

type codexRequest struct {
	Model             string              `json:"model"`
	Stream            bool                `json:"stream"`
	Input             []codexItem         `json:"input"`
	ToolChoice        string              `json:"tool_choice"`
	ParallelToolCalls bool                `json:"parallel_tool_calls"`
	Reasoning         codexReasoning      `json:"reasoning"`
	Store             bool                `json:"store"`
	Include           []string            `json:"include"`
	PromptCacheKey    string              `json:"prompt_cache_key"`
	Text              map[string]string   `json:"text"`
	ClientMetadata    codexClientMetadata `json:"client_metadata"`
}

type codexTurnMetadata struct {
	InstallationID             string `json:"installation_id"`
	SessionID                  string `json:"session_id"`
	ThreadID                   string `json:"thread_id"`
	AgentName                  string `json:"agent_name"`
	TurnID                     string `json:"turn_id"`
	WindowID                   string `json:"window_id"`
	WindowNumber               int    `json:"window_number"`
	ContextWindowID            string `json:"context_window_id"`
	RequestKind                string `json:"request_kind"`
	HistoryInitialization      string `json:"history_initialization"`
	RootTurnID                 string `json:"root_turn_id"`
	ThreadSource               string `json:"thread_source"`
	TurnTrigger                string `json:"turn_trigger"`
	Sandbox                    string `json:"sandbox"`
	SandboxMode                string `json:"sandbox_mode"`
	AutoReviewEnabled          bool   `json:"auto_review_enabled"`
	NodeReplAutoReviewRequired bool   `json:"node_repl_auto_review_required"`
	TurnStartedAtUnixMS        int64  `json:"turn_started_at_unix_ms"`
	AnalyticsEnabled           bool   `json:"analytics_enabled"`
	Model                      string `json:"model"`
	ReasoningEffort            string `json:"reasoning_effort,omitempty"`
}

// codexTurn returns the request and headers of a new Codex thread whose first user message is prompt.
func codexTurn(authID, model, effort, instructions, prompt string) (codexRequest, http.Header) {
	now := time.Now()
	thread, turnID := uuidV7(), uuidV7().String()
	threadID := thread.String()
	windowID := threadID + ":0"
	// Each credential acts as its own Codex installation, stable across restarts.
	installation := sha256.Sum256([]byte("codex-installation:" + authID))
	installationID := newUUID(installation[:], 4).String()
	metadata, _ := json.Marshal(codexTurnMetadata{
		InstallationID: installationID, SessionID: threadID, ThreadID: threadID, AgentName: "/root",
		TurnID: turnID, WindowID: windowID, ContextWindowID: uuidV7().String(), RequestKind: "turn",
		HistoryInitialization: "new", RootTurnID: turnID, ThreadSource: "user", TurnTrigger: "user",
		Sandbox: "seccomp", SandboxMode: "workspace-write", AutoReviewEnabled: true, NodeReplAutoReviewRequired: true,
		TurnStartedAtUnixMS: now.UnixMilli(), AnalyticsEnabled: true, Model: model, ReasoningEffort: effort,
	})
	created := float64(now.UnixMicro()) / 1e6
	input := codexPrefix(thread)
	for i, message := range codexContext.Messages {
		parts := message.Content
		if i == 0 && instructions != "" {
			parts = append([]codexText{{"generic.developer_instructions", instructions}}, parts...)
		}
		input = append(input, codexMessage("msg_"+uuidV7().String(), message.Role, turnID, created, parts...))
	}
	input = append(input,
		codexMessage("msg_"+uuidV7().String(), "user", turnID, created,
			codexText{"environments.environment_context", fmt.Sprintf(codexEnvironment, now.UTC().Format(time.DateOnly))}),
		codexMessage("msg_"+uuidV7().String(), "user", turnID, created, codexText{"user.text", prompt}),
	)
	request := codexRequest{
		Model: model, Input: input, ToolChoice: "auto", Reasoning: codexReasoning{effort, "all_turns"},
		Stream: true, Include: []string{"reasoning.encrypted_content"}, PromptCacheKey: threadID,
		Text: map[string]string{"verbosity": "low"},
		ClientMetadata: codexClientMetadata{
			ThreadID: threadID, WindowID: windowID, TurnID: turnID, RootTurnID: turnID,
			TurnMetadata: string(metadata), InstallationID: installationID, SessionID: threadID,
		},
	}
	headers := http.Header{
		"Accept":                                 {"text/event-stream"},
		"Content-Type":                           {"application/json"},
		"Originator":                             {"codex-tui"},
		"User-Agent":                             {codexResponsesUserAgent},
		"Session-Id":                             {threadID},
		"Thread-Id":                              {threadID},
		"X-Client-Request-Id":                    {threadID},
		"X-Codex-Window-Id":                      {windowID},
		"X-Codex-Turn-Metadata":                  {string(metadata)},
		"X-Codex-Beta-Features":                  {"remote_compaction_v2"},
		"X-Openai-Internal-Codex-Responses-Lite": {"true"},
	}
	return request, headers
}

// codexPrefix builds the tools and base instructions items Codex puts first in every request.
func codexPrefix(thread evalUUID) []codexItem {
	space := uuidV5(uuidOID, []byte(thread.String()))
	instructions := codexContext.BaseInstructions
	return []codexItem{
		{Type: "additional_tools", ID: "at_" + uuidV5(space, codexTools).String(), Role: "developer", Tools: codexTools},
		codexMessage("msg_"+uuidV5(space, []byte(instructions)).String(), "developer", "", 0, codexText{"model.base_instructions", instructions}),
	}
}

func codexMessage(id, role, turnID string, created float64, parts ...codexText) codexItem {
	metadata := &codexItemMetadata{TurnID: turnID, CreateTime: created}
	item := codexItem{Type: "message", ID: id, Role: role, Metadata: metadata}
	for _, part := range parts {
		item.Content = append(item.Content, codexContent{"input_text", part.Text})
		metadata.ContentItemKinds = append(metadata.ContentItemKinds, part.Kind)
	}
	return item
}

type evalUUID [16]byte

// uuidOID is the RFC 9562 name space for ISO OIDs.
var uuidOID = evalUUID{0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

func newUUID(b []byte, version byte) (id evalUUID) {
	copy(id[:], b)
	id[6] = id[6]&0x0f | version<<4
	id[8] = id[8]&0x3f | 0x80
	return id
}

func uuidV5(space evalUUID, name []byte) evalUUID {
	sum := sha1.Sum(append(space[:], name...))
	return newUUID(sum[:], 5)
}

func uuidV7() evalUUID {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixMilli())<<16)
	_, _ = rand.Read(b[6:])
	return newUUID(b[:], 7)
}

func (id evalUUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
}

// ---------------------------------------------------------------------------
// Host model execution over host.model.* RPC.
// ---------------------------------------------------------------------------

// evalUsage counts the tokens of one or more model responses.
type evalUsage struct {
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

func (u *evalUsage) add(v evalUsage) {
	u.InputTokens += v.InputTokens
	u.OutputTokens += v.OutputTokens
	u.ReasoningTokens += v.ReasoningTokens
}

type evalModelResponse struct {
	Answer string
	evalUsage
}

// evalRetryableError marks a failure that sending the request again may avoid.
type evalRetryableError struct{ error }

func (e evalRetryableError) Unwrap() error { return e.error }

// hostCallRaw performs a host RPC and returns the raw result envelope payload.
func hostCallRaw(method string, payload any) (json.RawMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s request: %w", method, err)
	}
	raw, err := callHostRaw(method, data)
	if err != nil {
		return nil, err
	}
	var env pluginabi.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode %s envelope: %w", method, err)
	}
	if !env.OK {
		if env.Error != nil {
			return nil, &evalHostError{Code: env.Error.Code, Message: env.Error.Message, HTTPStatus: env.Error.HTTPStatus}
		}
		return nil, fmt.Errorf("%s failed", method)
	}
	return env.Result, nil
}

// evalHostError is the host error surfaced by host.model.execute_stream.
type evalHostError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *evalHostError) Error() string {
	if e.Message != "" {
		return e.Code + ": " + e.Message
	}
	return e.Code
}

// executeResponse sends a Responses request through CPA on the selected credential.
func executeResponse(ctx context.Context, authID, provider, model string, body any, headers http.Header) (result evalResponseBody, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = evalResponseBody{}
			err = fmt.Errorf("模型请求异常：%v", recovered)
		}
	}()
	if strings.TrimSpace(authID) == "" || strings.TrimSpace(provider) == "" {
		return result, fmt.Errorf("缺少凭证 ID 或提供商，无法指定凭证")
	}
	// Keep <, > and & literal, as Codex sends them.
	var request bytes.Buffer
	encoder := json.NewEncoder(&request)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		return result, err
	}
	req := pluginapi.HostModelExecutionRequest{
		EntryProtocol:  "openai-response",
		ExitProtocol:   "openai-response",
		Model:          model,
		Stream:         true,
		Body:           bytes.TrimSuffix(request.Bytes(), []byte("\n")),
		Headers:        headers,
		ForcedProvider: provider,
		AuthID:         authID,
	}
	raw, err := hostCallRaw(pluginabi.MethodHostModelExecuteStream, req)
	if err != nil {
		var rejected *evalHostError
		if errors.As(err, &rejected) && (rejected.HTTPStatus == http.StatusRequestTimeout || rejected.HTTPStatus >= http.StatusInternalServerError) {
			return result, evalRetryableError{err}
		}
		return result, err
	}
	var stream struct {
		ID string `json:"stream_id"`
	}
	if err := json.Unmarshal(raw, &stream); err != nil || stream.ID == "" {
		return result, fmt.Errorf("解析 CPA 响应失败：%s", raw)
	}
	// The host closes a stream by itself only once it is read to the end. Closing it early ends the pending
	// read and cancels the upstream request.
	closeStream := sync.OnceFunc(func() {
		_, _ = hostCallRaw(pluginabi.MethodHostModelStreamClose, pluginapi.HostModelStreamCloseRequest{StreamID: stream.ID})
	})
	defer closeStream()
	stop := context.AfterFunc(ctx, closeStream)
	defer stop()
	result, err = readResponseStream(stream.ID)
	if err != nil && ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	if err != nil {
		return result, evalRetryableError{err}
	}
	return result, nil
}

type evalResponseItem struct {
	Type    string `json:"type"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

type evalResponseBody struct {
	Status string             `json:"status"`
	Error  json.RawMessage    `json:"error"`
	Output []evalResponseItem `json:"output"`
	Usage  struct {
		InputTokens         int64 `json:"input_tokens"`
		OutputTokens        int64 `json:"output_tokens"`
		OutputTokensDetails struct {
			ReasoningTokens int64 `json:"reasoning_tokens"`
		} `json:"output_tokens_details"`
	} `json:"usage"`
}

func (r evalResponseBody) completed() bool {
	return r.Status == "completed" && (len(r.Error) == 0 || string(r.Error) == "null")
}

func (r evalResponseBody) tokenUsage() evalUsage {
	return evalUsage{r.Usage.InputTokens, r.Usage.OutputTokens, r.Usage.OutputTokensDetails.ReasoningTokens}
}

// executeModel requires a completed text answer; unusable answers may be retried.
func executeModel(ctx context.Context, authID, provider, model string, body any, headers http.Header) (evalModelResponse, error) {
	out, err := executeResponse(ctx, authID, provider, model, body, headers)
	if err != nil {
		return evalModelResponse{}, err
	}
	result, err := answerOf(out)
	if err != nil {
		return result, evalRetryableError{err}
	}
	return result, nil
}

// answerOf returns the answer text and usage of a final response.
func answerOf(out evalResponseBody) (evalModelResponse, error) {
	result := evalModelResponse{evalUsage: out.tokenUsage()}
	if !out.completed() {
		return result, fmt.Errorf("模型响应未完成（%s）：%s", out.Status, out.Error)
	}
	var answer strings.Builder
	for _, item := range out.Output {
		if item.Type != "message" {
			continue
		}
		for _, part := range item.Content {
			if part.Type == "output_text" || part.Type == "text" {
				answer.WriteString(part.Text)
			}
		}
	}
	if strings.TrimSpace(answer.String()) == "" {
		if slices.ContainsFunc(out.Output, func(item evalResponseItem) bool { return strings.HasSuffix(item.Type, "_call") }) {
			return result, fmt.Errorf("模型调用了工具，没有直接回答")
		}
		return result, fmt.Errorf("模型未返回文本")
	}
	result.Answer = answer.String()
	return result, nil
}

// evalStreamMaxChunks / evalStreamMaxBytes bound the response stream read so a
// host that never reports done/error cannot block the eval loop forever.
const (
	evalStreamMaxChunks = 10000
	evalStreamMaxBytes  = 32 << 20
)

// readResponseStream reads a Responses event stream to its end and returns the final response.
func readResponseStream(id string) (evalResponseBody, error) {
	var out evalResponseBody
	var events []byte
	chunks := 0
	for done := false; !done; {
		if len(events) > evalStreamMaxBytes {
			return out, fmt.Errorf("模型响应流过大，已中断读取")
		}
		if chunks >= evalStreamMaxChunks {
			return out, fmt.Errorf("模型响应流读取超限，已中断读取")
		}
		chunks++
		raw, err := hostCallRaw(pluginabi.MethodHostModelStreamRead, pluginapi.HostModelStreamReadRequest{StreamID: id})
		if err != nil {
			return out, err
		}
		var chunk struct {
			Payload []byte `json:"payload"`
			Error   string `json:"error"`
			Done    bool   `json:"done"`
		}
		if err := json.Unmarshal(raw, &chunk); err != nil {
			return out, fmt.Errorf("解析模型响应流失败：%w", err)
		}
		if chunk.Error != "" {
			return out, fmt.Errorf("模型响应流中断：%s", chunk.Error)
		}
		// A chunk may end mid-line, or start the next field without a line break.
		if len(events) > 0 && !bytes.HasSuffix(events, []byte("\n")) && startsSSEField(chunk.Payload) {
			events = append(events, '\n')
		}
		events = append(events, chunk.Payload...)
		done = chunk.Done
	}
	var items []evalResponseItem
	final := false
	for _, line := range bytes.Split(events, []byte("\n")) {
		data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte("data:"))
		if !ok {
			continue
		}
		var event struct {
			Type     string          `json:"type"`
			Response json.RawMessage `json:"response"`
			Item     json.RawMessage `json:"item"`
		}
		if json.Unmarshal(data, &event) != nil {
			continue
		}
		switch event.Type {
		case "response.output_item.done":
			var item evalResponseItem
			if json.Unmarshal(event.Item, &item) == nil {
				items = append(items, item)
			}
		case "response.completed", "response.incomplete", "response.failed":
			if raw := bytes.TrimSpace(event.Response); len(raw) == 0 || raw[0] != '{' {
				return out, fmt.Errorf("模型返回了无效的完成事件")
			}
			if err := json.Unmarshal(event.Response, &out); err != nil {
				return out, fmt.Errorf("解析模型响应失败：%w", err)
			}
			status := strings.TrimPrefix(event.Type, "response.")
			if out.Status != "" && out.Status != status {
				return out, fmt.Errorf("模型完成事件与响应状态不一致")
			}
			out.Status = status
			final = true
		case "error":
			return out, fmt.Errorf("模型返回错误：%s", bytes.TrimSpace(data))
		}
	}
	if !final {
		return out, fmt.Errorf("模型响应流在完成前中断")
	}
	if len(out.Output) == 0 {
		out.Output = items
	}
	return out, nil
}

func startsSSEField(chunk []byte) bool {
	chunk = bytes.TrimLeft(chunk, " \t")
	for _, field := range []string{"data:", "event:", "id:", "retry:", ":"} {
		if bytes.HasPrefix(chunk, []byte(field)) {
			return true
		}
	}
	return false
}

// mustDecodeEval panics on JSON decode failure (used for embedded static data).
func mustDecodeEval[T any](data []byte) T {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		panic(err)
	}
	return value
}

// truncateEval limits a string to limit runes with an ellipsis.
func truncateEval(text string, limit int) string {
	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit]) + "…"
}

// ---------------------------------------------------------------------------
// askCodex retry loop (candy + fingerprint share this path).
// ---------------------------------------------------------------------------

// Time limits of an attempt; a model stuck repeating itself streams until the upstream cuts it off.
const (
	evalChallengeTimeout = 3 * time.Minute
	evalAnswerTimeout    = 10 * time.Minute
)

// evalModelAttempts bounds per-challenge attempts; each retry waits twice as long.
const evalModelAttempts = 3

var evalRetryDelay = 2 * time.Second

// askCodexEval retries transient failures and answers rejected by accept, summing usage across attempts.
// CPA handles retries before streaming starts; the plugin also retries interrupted or unusable streams.
func askCodexEval(ctx context.Context, authID, provider, model, effort, instructions, prompt string, limit time.Duration, accept func(answer string) error) (out evalModelResponse, attempts int, err error) {
	if effort == "none" {
		effort = ""
	}
	body, headers := codexTurn(authID, model, effort, instructions, prompt)
	timeout := fmt.Errorf("模型 %d 分钟内未完成回答，已中断请求", int(limit.Minutes()))
	for delay := evalRetryDelay; ; delay *= 2 {
		if err = ctx.Err(); err != nil {
			return
		}
		attempts++
		requestCtx, cancel := context.WithTimeoutCause(ctx, limit, timeout)
		var response evalModelResponse
		response, err = executeModel(requestCtx, authID, provider, model, body, headers)
		cancel()
		out.Answer = response.Answer
		out.add(response.evalUsage)
		if err == nil && accept != nil {
			if err = accept(response.Answer); err != nil {
				err = evalRetryableError{err}
			}
		}
		if err == nil || attempts == evalModelAttempts || !errors.As(err, new(evalRetryableError)) {
			return
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return out, attempts, ctx.Err()
		}
	}
}
