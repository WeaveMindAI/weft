import { StreamLanguage } from '@codemirror/language';

/**
 * Weft DSL stream-mode language definition for CodeMirror 6.
 * Token function returns CodeMirror style strings (mapped to highlight classes).
 */
const weftLanguage = StreamLanguage.define({
	startState() {
		return { inMultiLine: false };
	},

	token(stream, state: { inMultiLine: boolean }) {
		// Multi-line value (``` ... ```)
		if (state.inMultiLine) {
			if (stream.eatSpace()) return 'string';
			if (stream.match(/^```/)) {
				state.inMultiLine = false;
				return 'bracket';
			}
			stream.skipToEnd();
			return 'string';
		}

		if (stream.eatSpace()) return null;

		// Comments
		if (stream.match(/^#.*/)) {
			return 'comment';
		}

		// Triple backtick opener (multiline string)
		if (stream.match(/^```/)) {
			// Check if this is an inline triple-backtick: ```content```
			// If the rest of the line has another ```, it's inline
			if (stream.match(/.*```/, false)) {
				// Inline: consume until closing ```
				stream.skipTo('`');
				stream.match(/^```/);
				return 'string';
			}
			state.inMultiLine = true;
			return 'bracket';
		}

		// Arrow (port signature separator)
		if (stream.match(/^->/)) {
			return 'operator';
		}

		// Braces
		if (stream.match(/^[{}]/)) {
			return 'brace';
		}

		// Parentheses (port signatures)
		if (stream.match(/^[()]/)) {
			return 'brace';
		}

		// Equals sign (connection or declaration)
		if (stream.match(/^=/)) {
			return 'operator';
		}

		// Question mark (optional marker)
		if (stream.match(/^\?/)) {
			return 'keyword';
		}

		// Dot (port separator)
		if (stream.match(/^\./)) {
			return 'punctuation';
		}

		// Colon (config separator)
		if (stream.match(/^:/)) {
			return 'punctuation';
		}

		// Comma
		if (stream.match(/^,/)) {
			return 'punctuation';
		}

		// Quoted string
		if (stream.match(/^"(?:[^"\\]|\\.)*"/)) {
			return 'string';
		}

		// Boolean
		if (stream.match(/^(true|false)(?=\s|$|,|\}|\))/)) {
			return 'bool';
		}

		// Number
		if (stream.match(/^-?\d+(\.\d+)?(?=\s|$|,|\}|\))/)) {
			return 'number';
		}

		// @require_one_of directive
		if (stream.match(/^@require_one_of/)) {
			return 'keyword';
		}

		// Identifier
		if (stream.match(/^[a-zA-Z_][a-zA-Z0-9_]*/)) {
			const word = stream.current();
			// Keywords
			if (word === 'self' || word === 'Group') {
				return 'keyword';
			}
			// PascalCase = node type name
			if (/^[A-Z]/.test(word)) {
				return 'typeName';
			}
			return 'variableName';
		}

		// JSON brackets
		if (stream.match(/^[\[\]]/)) {
			return 'squareBracket';
		}

		// Pipe (union type separator)
		if (stream.match(/^\|/)) {
			return 'operator';
		}

		stream.next();
		return null;
	},
});

export function weft() {
	return weftLanguage;
}
