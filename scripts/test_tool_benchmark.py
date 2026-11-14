"""Check real stream reconstruction and upstream grading before model use."""
import json
import time
import unittest

from tool_benchmark import canonical_tools, grade, openapi_schema, read_sse


class ToolBenchmarkTests(unittest.TestCase):
    def test_official_grader_rejects_wrong_names_values_and_missing_calls(self):
        item = {"function": [{"name": "math.add", "parameters": {
            "properties": {"x": {"type": "integer"}}, "required": ["x"]}}],
            "category": "simple_python", "gold": [{"math.add": {"x": [2]}}]}
        self.assertTrue(grade(item, [{"math_add": {"x": 2}}])["valid"])
        for calls in ([], [{"math_add": {"x": 3}}], [{"wrong": {"x": 2}}]):
            self.assertFalse(grade(item, calls)["valid"])

    def test_schema_type_conversion_preserves_property_names(self):
        schema = {"type": "dict", "properties": {"type": {"type": "float"},
                  "values": {"type": "tuple", "items": {"type": "integer"}}}}
        value = openapi_schema(schema)
        self.assertEqual(value["type"], "object")
        self.assertEqual(value["properties"]["type"]["type"], "number")
        self.assertEqual(value["properties"]["values"]["type"], "array")
        self.assertEqual(list(value), sorted(value))

    def test_interleaved_tool_fragments_are_joined_by_index(self):
        def chunk(calls):
            return ("data: " + json.dumps({"choices": [{"delta": {"tool_calls": calls}}]}) + "\n").encode()
        lines = [chunk([{"index": 0, "id": "a", "function": {"name": "one", "arguments": '{"x":'}},
                        {"index": 1, "id": "b", "function": {"name": "two", "arguments": '{"y":'}}]),
                 chunk([{"index": 1, "function": {"arguments": "2}"}},
                        {"index": 0, "function": {"arguments": "1}"}}]),
                 b'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}\n',
                 b'data: [DONE]\n']
        value = read_sse(lines, time.monotonic())
        self.assertEqual(canonical_tools(value["tool_calls"]), [{"one": {"x": 1}}, {"two": {"y": 2}}])
        self.assertEqual(value["usage"]["total_tokens"], 12)
        self.assertTrue(value["done_received"])

    def test_truncated_stream_and_invalid_arguments_fail(self):
        with self.assertRaises(ValueError):read_sse([], time.monotonic())
        with self.assertRaises(ValueError):canonical_tools([{"function": {"name": "x", "arguments": '{"a":'}}])


if __name__ == "__main__":unittest.main()
