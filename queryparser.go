// Copyright (c) 2013-2024 Jack Christensen, 2025 George Steel
// Forked from https://github.com/jackc/pgx/blob/master/named_args.go
// SPDX-License-Identifier: MIT

package pgxx

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// NamedArgs can be used as the first argument to a query method. It will replace every '@' named placeholder with a '$'
// ordinal placeholder and construct the appropriate arguments.
//
// For example, the following two queries are equivalent:
//
//	conn.Query(ctx, "select * from widgets where foo = @foo and bar = @bar", pgx.NamedArgs{"foo": 1, "bar": 2})
//	conn.Query(ctx, "select * from widgets where foo = $1 and bar = $2", 1, 2)
//
// Named placeholders are case sensitive and must start with a letter or underscore. Subsequent characters can be
// letters, numbers, or underscores.

type namedArg string

type sqlLexer struct {
	src     string
	start   int
	pos     int
	nested  int // multiline comment nesting level.
	stateFn stateFn
	parts   []any

	nameToOrdinal map[namedArg]int
}

type stateFn func(*sqlLexer) stateFn

func RewriteNamedQuery(namedQuery SQL) (SQL, []FieldName) {
	l := &sqlLexer{
		src:           string(namedQuery),
		stateFn:       rawState,
		nameToOrdinal: make(map[namedArg]int),
	}

	for l.stateFn != nil {
		l.stateFn = l.stateFn(l)
	}

	sb := strings.Builder{}
	for _, p := range l.parts {
		switch p := p.(type) {
		case string:
			sb.WriteString(p)
		case namedArg:
			sb.WriteRune('$')
			sb.WriteString(strconv.Itoa(l.nameToOrdinal[p]))
		}
	}

	newArgs := make([]FieldName, len(l.nameToOrdinal))
	for name, ordinal := range l.nameToOrdinal {
		newArgs[ordinal-1] = FieldName(name)
	}

	return SQL(sb.String()), newArgs
}

func rawState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case 'e', 'E':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '\'' {
				l.pos += width
				return escapeStringState
			}
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '@':
			nextRune, _ := utf8.DecodeRuneInString(l.src[l.pos:])
			if isLetter(nextRune) || nextRune == '_' {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos-width])
				}
				l.start = l.pos
				return namedArgState
			}
		case '-':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '-' {
				l.pos += width
				return oneLineCommentState
			}
		case '/':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '*' {
				l.pos += width
				return multilineCommentState
			}
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func namedArgState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		if r == utf8.RuneError {
			if l.pos-l.start > 0 {
				na := namedArg(l.src[l.start:l.pos])
				if _, found := l.nameToOrdinal[na]; !found {
					l.nameToOrdinal[na] = len(l.nameToOrdinal) + 1
				}
				l.parts = append(l.parts, na)
				l.start = l.pos
			}
			return nil
		} else if !(isLetter(r) || (r >= '0' && r <= '9') || r == '_') {
			l.pos -= width
			na := namedArg(l.src[l.start:l.pos])
			if _, found := l.nameToOrdinal[na]; !found {
				l.nameToOrdinal[na] = len(l.nameToOrdinal) + 1
			}
			l.parts = append(l.parts, namedArg(na))
			l.start = l.pos
			return rawState
		}
	}
}

func singleQuoteState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\'':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '\'' {
				return rawState
			}
			l.pos += width
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func doubleQuoteState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '"':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '"' {
				return rawState
			}
			l.pos += width
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func escapeStringState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\\':
			_, width = utf8.DecodeRuneInString(l.src[l.pos:])
			l.pos += width
		case '\'':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '\'' {
				return rawState
			}
			l.pos += width
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func oneLineCommentState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '\\':
			_, width = utf8.DecodeRuneInString(l.src[l.pos:])
			l.pos += width
		case '\n', '\r':
			return rawState
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func multilineCommentState(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '/':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '*' {
				l.pos += width
				l.nested++
			}
		case '*':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune != '/' {
				continue
			}

			l.pos += width
			if l.nested == 0 {
				return rawState
			}
			l.nested--

		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}
