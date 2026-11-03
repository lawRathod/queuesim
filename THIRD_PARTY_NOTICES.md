# Third-party notices

`queuesim` itself is MIT-licensed (see [LICENSE](LICENSE)).

## GoDES simulation engine

The packages under [`godes/`](godes/) are a source fork of the GoDES
deterministic simulation engine, copied from GoDES commit
`8817f3d9c15bc544829d373e0205dba38318299a` and re-published under this
module's import path. Only the engine packages used by this project were
copied; import paths were rewritten.

```
MIT License

Copyright (c) 2026 Prateek Rathod

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## No runtime dependencies

The Go module has no third-party requirements. The web dashboard is plain
HTML, CSS, and JavaScript embedded with `go:embed`; it loads nothing from a
CDN. There are no fonts, icons, or images from third parties.

## Names

Temporal and Tasqueue are names/trademarks of their respective owners. The
models in `models/` are independent, unofficial recreations for education and
are not affiliated with or endorsed by those projects.
