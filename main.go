// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	webflag "github.com/prometheus/exporter-toolkit/web/kingpinflag"

	dto "github.com/prometheus/client_model/go"
	promlogflag "github.com/prometheus/common/promlog/flag"

	api_v1 "github.com/prometheus/pushgateway/api/v1"
	"github.com/prometheus/pushgateway/cgroup/parser"
	"github.com/prometheus/pushgateway/handler"
	"github.com/prometheus/pushgateway/internal/monitor"
	"github.com/prometheus/pushgateway/storage"
	"toolman.org/net/peercred"
)

func init() {
	prometheus.MustRegister(version.NewCollector("pushgateway"))
}

// logFunc in an adaptor to plug gokit logging into promhttp.HandlerOpts.
type logFunc func(...interface{}) error

func (lf logFunc) Println(v ...interface{}) {
	lf("msg", fmt.Sprintln(v...))
}

func main() {
	var (
		app                 = kingpin.New(filepath.Base(os.Args[0]), "The Pushgateway")
		webConfig           = webflag.AddFlags(app, ":9091")
		metricsPath         = app.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		externalURL         = app.Flag("web.external-url", "The URL under which the Pushgateway is externally reachable.").Default("").URL()
		routePrefix         = app.Flag("web.route-prefix", "Prefix for the internal routes of web endpoints. Defaults to the path of --web.external-url.").Default("").String()
		enableAdminAPI      = app.Flag("web.enable-admin-api", "Enable API endpoints for admin control actions.").Default("false").Bool()
		persistenceFile     = app.Flag("persistence.file", "File to persist metrics. If empty, metrics are only kept in memory.").Default("").String()
		persistenceInterval = app.Flag("persistence.interval", "The minimum interval at which to write out the persistence file.").Default("5m").Duration()
		promlogConfig       = promlog.Config{}
		socketPath          = app.Flag("socket.path", "Socket path for communication.").Default("/tmp/pushgateway/socket").String()
		trustedPath         = app.Flag("trust.path", "Multiple trusted apptainer starter paths, use ';' to separate multiple entries").Default("").String()
		monitorInterval     = app.Flag("monitor.inverval", "The internval for sending system status.").Default("0.5s").Duration()
	)
	promlogflag.AddFlags(app, &promlogConfig)
	app.Version(version.Print("pushgateway"))
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := promlog.New(&promlogConfig)

	*routePrefix = computeRoutePrefix(*routePrefix, *externalURL)
	externalPathPrefix := computeRoutePrefix("", *externalURL)

	level.Info(logger).Log("msg", "starting pushgateway", "version", version.Info())
	level.Info(logger).Log("build_context", version.BuildContext())
	level.Debug(logger).Log("msg", "external URL", "url", *externalURL)
	level.Debug(logger).Log("msg", "path prefix used externally", "path", externalPathPrefix)
	level.Debug(logger).Log("msg", "path prefix for internal routing", "path", *routePrefix)

	// flags is used to show command line flags on the status page.
	// Kingpin default flags are excluded as they would be confusing.
	flags := map[string]string{}
	boilerplateFlags := kingpin.New("", "").Version("")
	for _, f := range app.Model().Flags {
		if boilerplateFlags.GetFlag(f.Name) == nil {
			flags[f.Name] = f.Value.String()
		}
	}

	ms := storage.NewDiskMetricStore(*persistenceFile, *persistenceInterval, prometheus.DefaultGatherer, logger)

	// Create a Gatherer combining the DefaultGatherer and the metrics from the metric store.
	g := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return ms.GetMetricFamilies(), nil }),
	}

	// server error channel
	errCh := make(chan error, 2)

	// verification server route
	r := route.New()
	mux := http.NewServeMux()
	mux.Handle("/", decodeRequest(r))
	verifyServer := &http.Server{Handler: mux}

	// create necessary parent folder for socket path
	parentFolder := path.Dir(*socketPath)
	if _, err := os.Stat(parentFolder); os.IsNotExist(err) {
		err := os.MkdirAll(parentFolder, 0o755)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create parent folder", "err", err)
		}
	}

	verificationOption := &serverOption{
		server:      verifyServer,
		webConfig:   webConfig,
		ms:          ms,
		logger:      logger,
		socketPath:  *socketPath,
		trustedPath: *trustedPath,
		interval:    time.NewTicker(*monitorInterval),
		errCh:       errCh,
	}
	go startVerificationServer(verificationOption)

	// metrics server
	metricsRoute := route.New()
	metricsRoute.Get(
		path.Join(*routePrefix, *metricsPath),
		promhttp.HandlerFor(g, promhttp.HandlerOpts{
			ErrorLog: logFunc(level.Error(logger).Log),
		}).ServeHTTP,
	)
	metricsRoute.Get(
		*routePrefix+"/healthy",
		handler.Healthy(ms).ServeHTTP,
	)
	metricsRoute.Get(
		*routePrefix+"/ready",
		handler.Ready(ms).ServeHTTP,
	)
	metricMux := http.NewServeMux()
	metricMux.Handle("/", decodeRequest(metricsRoute))

	buildInfo := map[string]string{
		"version":   version.Version,
		"revision":  version.Revision,
		"branch":    version.Branch,
		"buildUser": version.BuildUser,
		"buildDate": version.BuildDate,
		"goVersion": version.GoVersion,
	}

	apiv1 := api_v1.New(logger, ms, flags, buildInfo)

	apiPath := "/api"
	if *routePrefix != "/" {
		apiPath = *routePrefix + apiPath
	}

	av1 := route.New()
	apiv1.Register(av1)
	if *enableAdminAPI {
		av1.Put("/admin/wipe", handler.WipeMetricStore(ms, logger).ServeHTTP)
	}

	metricMux.Handle(apiPath+"/v1/", http.StripPrefix(apiPath+"/v1", av1))
	metricServer := &http.Server{Handler: metricMux}

	metricOption := &serverOption{
		server:    metricServer,
		webConfig: webConfig,
		ms:        ms,
		logger:    logger,
		errCh:     errCh,
	}
	go startMetricsServer(metricOption)

	err := shutdownServerOnQuit(*socketPath, []*serverOption{verificationOption, metricOption}, ms, errCh, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to clean up the server", "err", err)
	}
}

func decodeRequest(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close() // Make sure the underlying io.Reader is closed.
		switch contentEncoding := r.Header.Get("Content-Encoding"); strings.ToLower(contentEncoding) {
		case "gzip":
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gr.Close()
			r.Body = gr
		case "snappy":
			r.Body = io.NopCloser(snappy.NewReader(r.Body))
		default:
			// Do nothing.
		}

		h.ServeHTTP(w, r)
	})
}

func handlePprof(w http.ResponseWriter, r *http.Request) {
	switch route.Param(r.Context(), "pprof") {
	case "/cmdline":
		pprof.Cmdline(w, r)
	case "/profile":
		pprof.Profile(w, r)
	case "/symbol":
		pprof.Symbol(w, r)
	default:
		pprof.Index(w, r)
	}
}

// computeRoutePrefix returns the effective route prefix based on the
// provided flag values for --web.route-prefix and
// --web.external-url. With prefix empty, the path of externalURL is
// used instead. A prefix "/" results in an empty returned prefix. Any
// non-empty prefix is normalized to start, but not to end, with "/".
func computeRoutePrefix(prefix string, externalURL *url.URL) string {
	if prefix == "" {
		prefix = externalURL.Path
	}

	if prefix == "/" {
		prefix = ""
	}

	if prefix != "" {
		prefix = "/" + strings.Trim(prefix, "/")
	}

	return prefix
}

// shutdownServerOnQuit shutdowns the provided server upon closing the provided
// quitCh or upon receiving a SIGINT or SIGTERM.
func shutdownServerOnQuit(socketPath string, options []*serverOption, ms *storage.DiskMetricStore, errCh <-chan error, logger log.Logger) error {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, os.Interrupt, syscall.SIGTERM)

	select {
	case <-notifier:
		level.Info(logger).Log("msg", "received SIGINT/SIGTERM; exiting gracefully...")
		break
	case err := <-errCh:
		level.Warn(logger).Log("msg", "received error when launching server, exiting gracefully...", "err", err)
		break
	}

	defer os.Remove(socketPath)

	var retErr error
	for _, option := range options {
		err := option.server.Shutdown(context.Background())
		if err != nil {
			level.Error(logger).Log("msg", "unable to shutdown the server", "err", err)
			retErr = fmt.Errorf("%w %w", retErr, err)
		}
	}

	err := ms.Shutdown()
	if err != nil {
		level.Error(logger).Log("msg", "unable to shutdown the storage service", "err", err)
		retErr = fmt.Errorf("%w %w", retErr, err)
	}
	return retErr
}

type serverOption struct {
	server      *http.Server
	webConfig   *web.FlagConfig
	ms          storage.MetricStore
	logger      log.Logger
	socketPath  string
	trustedPath string
	interval    *time.Ticker
	errCh       chan error
}

func startVerificationServer(option *serverOption) {
	level.Info(option.logger).Log("msg", "Start verification server")
	unixListener, err := peercred.Listen(context.Background(), option.socketPath)
	if err != nil {
		level.Error(option.logger).Log("msg", "Could not create local unix socket", "err", err)
		option.errCh <- err
		return
	}

	// chmod socketPath
	err = os.Chmod(option.socketPath, 0o777)
	if err != nil {
		level.Error(option.logger).Log("msg", "Could not chmod local unix socket", "err", err)
		option.errCh <- err
		return
	}

	listener := wrappedListener{
		Listener:     unixListener,
		trustedPath:  option.trustedPath,
		option:       option,
		containerMap: make(map[string]*containerInfo),
	}

	quitCh := make(chan struct{}, 1)

	go func() {
		err = web.Serve(&listener, option.server, option.webConfig, option.logger)
		if err != nil {
			if err == http.ErrServerClosed {
				level.Info(option.logger).Log("msg", "Verification server stopped")
			} else {
				level.Error(option.logger).Log("msg", "Verification server stopped with error", "err", err)
				option.errCh <- err
			}
		}

		quitCh <- struct{}{}
	}()

	for {
		for id, info := range listener.containerMap {
			select {
			case <-quitCh:
				// stop all loop
				return
			case err := <-info.ErrCh:
				level.Error(option.logger).Log("msg", "Container monitor instance receieved error", "container id", id, "err", err)
				// server side closes the connection in case client side misses (in theory client will close the connection first)
				if info.Conn != nil {
					info.Conn.Close()
				}
			case <-info.Done:
				level.Info(option.logger).Log("msg", "Container monitor instance completed, will close the connection", "container id", id)
				// server side closes the connection in case client side misses (in theory client will close the connection first)
				if info.Conn != nil {
					info.Conn.Close()
				}
			case <-time.NewTicker(time.Millisecond * 10).C:
				// check next monitor instance
			}
		}
	}
}

func startMetricsServer(option *serverOption) {
	level.Info(option.logger).Log("msg", "Start metrics server")
	err := web.ListenAndServe(option.server, option.webConfig, option.logger)
	if err != nil {
		if err == http.ErrServerClosed {
			level.Info(option.logger).Log("msg", "Metrics server stopped")
		} else {
			level.Error(option.logger).Log("msg", "Metrics server stopped", "err", err)
			option.errCh <- err
		}
	}
}

type containerInfo struct {
	*parser.ContainerInfo
	*monitor.MonitorInstance
	net.Conn
}

type wrappedListener struct {
	*peercred.Listener
	trustedPath  string
	option       *serverOption
	containerMap map[string]*containerInfo
}

func (l *wrappedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	pid := conn.(*peercred.Conn).Ucred.Pid

	// verification by pid
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return nil, err
	}

	exe := filepath.Base(link)
	verify := false
	for _, path := range strings.Split(l.trustedPath, ";") {
		if strings.TrimSpace(link) == strings.TrimSpace(path) {
			verify = true
		}
	}

	if !verify {
		if conn != nil {
			conn.Close()
		}
		level.Error(l.option.logger).Log("msg", fmt.Sprintf("%s is not trusted, connection rejected", link))
		return conn, nil
	}

	// container and monitor instance info
	container := &parser.ContainerInfo{
		FullPath: link,
		Pid:      uint64(pid),
		Exe:      exe,
		Id:       fmt.Sprintf("%s_%d", exe, pid),
	}
	instance := monitor.New(l.option.interval)

	// save the container info for further usage
	l.containerMap[container.Id] = &containerInfo{
		ContainerInfo:   container,
		MonitorInstance: instance,
		Conn:            conn,
	}

	// fire monitor thread
	go instance.Start(container, l.option.ms, l.option.logger)

	level.Info(l.option.logger).Log("msg", "New connection established", "container id", container.Id, "container pid", container.Pid, "container full path", container.FullPath)

	return conn, nil
}
