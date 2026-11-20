"""List the top-level Go tests of packages as `go test` itself finds them.

Every tool that needs "which top-level tests exist" asks the Go toolchain
instead of reading `func Test...` text: `go test -json -list ^Test` compiles the
test binary of each package (build constraints, import aliases, parameter
names, methods and the TestMain harness are all resolved by Go) and prints the
tests it would run. Listing executes no test body; package init and a TestMain
would run, and this repository defines no TestMain.

The listing runs per Go module with GOWORK=off, CGO_ENABLED=0 and
-mod=readonly, the settings of the CI product checks, under a bounded timeout.
A missing `go`, a non-zero exit, a build failure or any event this module does
not recognise raises ListError; nothing is silently returned as empty.
Results are cached for the life of the process.

The listing keeps Go's multiplicity: each package maps every listed name to
the number of times Go lists it. The internal and the external test package of
one directory may each declare a test of the same name; Go lists it twice and
runs both under that one name. A reader that only asks whether a name exists,
or where it is declared, reads the names. A reader that selects tests by name
for a run, or judges a run by its result lines, takes its names through
`selected_tests` (or checks them with `repeated`), which refuses a selected
name listed more than once: `-run` cannot select one of the two tests and a
`--- PASS: Name` line cannot say which of them it reports.

`go test -list` says which tests exist, not where they are declared. A check
that ties a test to a file uses `declares`: the name must be listed for the
package, the file must be one Go compiles for that package's tests (go list),
and Go's parser must find a top-level function of the test shape with that
name in that file (tools/quality/testdecls, which applies the go/build
context). A raw string, a comment, a method or a build-excluded file cannot
satisfy it. Only the declarations of the looked-up test are checked; other
functions of the file, such as repeated `init` functions, are not.
"""
import json
import os
from pathlib import Path
import posixpath
import re
import subprocess
from types import MappingProxyType

# The Go listings this tool runs: (subcommands that run Go, or None for every
# invocation; the module directories Go loads). tools/test_workflow_toolchains.py
# reads this declaration to require a Go that can list those modules.
# Here: declarations run tools/quality/testdecls.
GO_LISTING = (None, ("tools/quality",))

PATTERN = "^Test"
TIMEOUT = 600
QUALITY = Path(__file__).resolve().parent / "quality"
# Under the ^Test pattern every listed name is a test; examples, benchmarks
# and fuzz targets do not match it.
NAME = re.compile(r"Test\w*")
_CACHE = {}


class ListError(ValueError):
    """Go could not list the tests, or listed something unexpected."""


def _environment():
    return dict(os.environ, GOWORK="off", CGO_ENABLED="0")


def _go(module, arguments, timeout):
    try:
        return subprocess.run(["go"] + arguments, cwd=module, env=_environment(), text=True,
                              capture_output=True, timeout=timeout)
    except FileNotFoundError as error:
        raise ListError("go is required to list Go tests: %s" % error) from error
    except subprocess.TimeoutExpired as error:
        raise ListError("go %s timed out after %ds in %s" % (" ".join(arguments), timeout, module)) from error


def _module_path(module, timeout):
    result = _go(module, ["list", "-mod=readonly", "-m"], timeout)
    path = result.stdout.strip()
    if result.returncode or not path or "\n" in path:
        raise ListError("go list -m failed in %s: %s" % (module, result.stderr.strip()[-2000:]))
    return path


def _relative(module_path, package):
    if package == module_path:
        return "."
    if package.startswith(module_path + "/"):
        return package[len(module_path) + 1:]
    raise ListError("listed package %s is outside module %s" % (package, module_path))


def parse(text, module_path):
    """Map module-relative package directories to {listed test name: count}.

    Only the events and output lines `go test -json -list` writes for a
    successful listing are accepted. A name may be listed more than once: the
    internal and the external test package of one directory may each declare
    it, and go test runs both. The count keeps that; see `selected_tests`."""
    outputs, finals, problems = {}, {}, []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except ValueError as error:
            raise ListError("line %d of go test -json output is not JSON: %s" % (number, error)) from error
        if not isinstance(event, dict):
            raise ListError("line %d of go test -json output is not an event" % number)
        action = event.get("Action")
        if action == "build-output":
            problems.append(event.get("Output", "").rstrip())
            continue
        if action == "build-fail":
            raise ListError("build failed: %s\n%s" % (event.get("ImportPath"), "\n".join(problems)[-2000:]))
        package = event.get("Package")
        if not package or event.get("Test"):
            raise ListError("unexpected go test -json event on line %d: %s" % (number, line[:200]))
        if action == "start":
            outputs.setdefault(package, "")
        elif action == "output":
            outputs[package] = outputs.get(package, "") + event.get("Output", "")
        elif action in ("pass", "fail", "skip"):
            if package in finals:
                raise ListError("duplicate final event for %s" % package)
            finals[package] = action
        else:
            raise ListError("unexpected go test -json action %r for %s" % (action, package))
    listed = {}
    for package, output in outputs.items():
        names, empty = {}, False
        for line in output.splitlines():
            if NAME.fullmatch(line):
                names[line] = names.get(line, 0) + 1
            elif line.startswith("ok  \t%s\t" % package):
                continue
            elif line == "?   \t%s\t[no test files]" % package:
                empty = True
            else:
                raise ListError("unexpected listing output for %s: %r" % (package, line[:200]))
        final = finals.get(package)
        if final != "pass" and not (final == "skip" and empty and not names):
            raise ListError("listing of %s ended with %s" % (package, final or "no final event"))
        listed[_relative(module_path, package)] = MappingProxyType(names)
    if set(finals) - set(outputs):
        raise ListError("final event without a start for %s" % ", ".join(sorted(set(finals) - set(outputs))))
    return listed


def list_tests(module, packages, timeout=TIMEOUT):
    """{module-relative package directory: read-only {top-level test name: count}}.

    `module` is the directory of a go.mod; `packages` are package patterns
    relative to it ("./integration", "./..."). A named package that Go does not
    list is an error. Iterating a package's mapping gives each name once; the
    count is how many times Go lists it (see `selected_tests`)."""
    module = Path(module).resolve()
    packages = tuple(packages)
    key = (str(module), packages)
    if key not in _CACHE:
        module_path = _module_path(module, timeout)
        result = _go(module, ["test", "-mod=readonly", "-json", "-list", PATTERN] + list(packages), timeout)
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise ListError("go test -list failed in %s (exit %d): %s" % (module, result.returncode, detail[-2000:]))
        listed = parse(result.stdout, module_path)
        for package in packages:
            if "..." not in package:
                directory = _directory(package)
                if directory not in listed:
                    raise ListError("go test -list did not list package %s in %s" % (package, module))
        _CACHE[key] = listed
    return _CACHE[key]


def _directory(package):
    """A module-relative package pattern as the slash-separated key Go reports."""
    return posixpath.normpath(package.replace("\\", "/"))


def package_tests(module, package, timeout=TIMEOUT):
    """{listed top-level test name: count} of one package directory of a module."""
    directory = _directory(package)
    return list_tests(module, ("./" + directory,), timeout)[directory]


def repeated(listing, chosen):
    """[(package, name, count)], sorted, of the names `chosen(package, name)`
    selects in a `list_tests` result that Go lists more than once."""
    return sorted((package, name, count) for package, names in listing.items()
                  for name, count in names.items() if count > 1 and chosen(package, name))


def repeated_message(package, name, count):
    return ("%s is listed %d times for %s (the internal and the external test package both declare it): "
            "go test -run cannot select one of them and a result line cannot tell them apart"
            % (name, count, package))


def selected_tests(module, packages, chosen, timeout=TIMEOUT):
    """{package directory: frozenset of the listed names `chosen(package, name)` selects}.

    This is the listing for a reader that selects tests for a run or judges
    a run by result lines. A selected name that Go lists more than once
    raises ListError before any result is read; names that are not selected
    may repeat."""
    listing = list_tests(module, packages, timeout)
    problems = repeated(listing, chosen)
    if problems:
        raise ListError("; ".join(repeated_message(*problem) for problem in problems))
    return {package: frozenset(name for name in names if chosen(package, name))
            for package, names in listing.items()}


def test_files(module, packages, timeout=TIMEOUT):
    """{module-relative package directory: frozenset of the _test.go file names Go compiles}.

    These are the files of the default build that `go test` compiles; a file
    excluded by a build constraint is absent."""
    module = Path(module).resolve()
    packages = tuple(packages)
    key = ("files", str(module), packages)
    if key not in _CACHE:
        module_path = _module_path(module, timeout)
        result = _go(module, ["list", "-mod=readonly", "-json=ImportPath,TestGoFiles,XTestGoFiles"] + list(packages), timeout)
        if result.returncode:
            raise ListError("go list failed in %s (exit %d): %s" % (module, result.returncode, result.stderr.strip()[-2000:]))
        files, decoder, text, index = {}, json.JSONDecoder(), result.stdout, 0
        while True:
            while index < len(text) and text[index].isspace():
                index += 1
            if index == len(text):
                break
            try:
                package, index = decoder.raw_decode(text, index)
            except ValueError as error:
                raise ListError("go list -json output is not JSON: %s" % error) from error
            if not isinstance(package, dict) or not isinstance(package.get("ImportPath"), str):
                raise ListError("unexpected go list -json object")
            names = package.get("TestGoFiles", []) + package.get("XTestGoFiles", [])
            files[_relative(module_path, package["ImportPath"])] = frozenset(names)
        _CACHE[key] = files
    return _CACHE[key]


def declarations(directories, timeout=TIMEOUT):
    """{absolute directory: {file name: ((function name, has the test shape), ...)}}.

    Go's parser reads the _test.go files that the go/build context selects in
    each directory (tools/quality/testdecls); methods are not listed, and a
    file without a top-level function is absent. The functions keep their
    source order and are not required to have distinct names: Go permits
    repeated top-level `init` and `_` functions, and only a lookup decides
    which names matter (see `declares`). A directory without test files maps
    to an empty dictionary."""
    directories = [str(Path(directory).resolve()) for directory in directories]
    missing = sorted({directory for directory in directories if ("decls", directory) not in _CACHE})
    if missing:
        result = _go(QUALITY, ["run", "-mod=readonly", "./testdecls"] + missing, timeout)
        if result.returncode:
            raise ListError("testdecls failed (exit %d): %s" % (result.returncode, result.stderr.strip()[-2000:]))
        found = {directory: {} for directory in missing}
        for number, line in enumerate(result.stdout.splitlines(), 1):
            try:
                decl = json.loads(line)
                directory, file, name, test = decl["dir"], decl["file"], decl["name"], decl["test"]
            except (ValueError, TypeError, KeyError) as error:
                raise ListError("line %d of testdecls output is not a declaration: %s" % (number, error)) from error
            if directory not in found or not isinstance(test, bool):
                raise ListError("unexpected testdecls declaration on line %d: %s" % (number, line[:200]))
            found[directory].setdefault(file, []).append((name, test))
        for directory, files in found.items():
            _CACHE[("decls", directory)] = {file: tuple(decls) for file, decls in files.items()}
    return {directory: _CACHE[("decls", directory)] for directory in directories}


def declares(module, path, name, timeout=TIMEOUT):
    """True when `path` declares `name` as a top-level test that Go lists.

    `module` is the directory of the go.mod containing `path`. Go must list
    `name` for the package of `path`, compile `path` for that package's tests,
    and its parser must find `name` declared there with the test shape.
    Only the test-shaped declarations of `name` in `path` are counted; other
    functions of the file, repeated or not, play no part. More than one such
    declaration cannot compile and raises ListError."""
    module, path = Path(module).resolve(), Path(path).resolve()
    package = path.parent.relative_to(module).as_posix()
    if path.name not in test_files(module, ("./...",), timeout).get(package, ()):
        return False
    if name not in list_tests(module, ("./...",), timeout).get(package, ()):
        return False
    decls = declarations([path.parent], timeout)[str(path.parent)].get(path.name, ())
    count = decls.count((name, True))
    if count > 1:
        raise ListError("duplicate test declaration of %s in %s" % (name, path.name))
    return count == 1
