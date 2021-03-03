using System;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class StringUtilsTest
    {
        [Fact]
        public void TestSnake2Camel()
        {
            Assert.Equal("fooBar", StringUtils.ToCamel("foo_bar"));
            Assert.Equal("FooBar", StringUtils.ToCamel("foo_bar", true));
            Assert.Equal("fooBar", StringUtils.ToCamel("FOO_BAR"));
            Assert.Equal("FooBar", StringUtils.ToCamel("FOO_BAR", true));

            Assert.Equal("aFoo", StringUtils.ToCamel("a_foo"));
            Assert.Equal("AFoo", StringUtils.ToCamel("a_foo", true));

            Assert.Equal("aBC", StringUtils.ToCamel("a_b_c"));
            Assert.Equal("ABC", StringUtils.ToCamel("a_b_c", true));

            // without "_"
            Assert.Equal("abc", StringUtils.ToCamel("abc"));
            Assert.Equal("Abc", StringUtils.ToCamel("abc", true));
            Assert.Equal("fooBar", StringUtils.ToCamel("fooBar"));
            Assert.Equal("FooBar", StringUtils.ToCamel("fooBar", true));

            Assert.Equal("fooBar", StringUtils.ToCamel("foo__bar"));
            Assert.Equal("FooBar", StringUtils.ToCamel("foo__bar", true));
            Assert.Equal("fooBar", StringUtils.ToCamel("FOO__BAR"));
            Assert.Equal("FooBar", StringUtils.ToCamel("FOO__BAR", true));

            Assert.Equal("aaBbCc", StringUtils.ToCamel("aa___bb__cc"));
            Assert.Equal("AaBbCc", StringUtils.ToCamel("aa___bb__cc", true));

            Assert.Equal("", StringUtils.ToCamel(""));
            Assert.Equal("", StringUtils.ToCamel("", true));

            var ex = Assert.Throws<ArgumentNullException>(() => StringUtils.ToCamel(null));
            Assert.Contains("Parameter 's'", ex.Message);
        }

        [Fact]
        public void TestCamel2Snake()
        {
            Assert.Equal("foo_bar", StringUtils.ToSnake("fooBar"));
            Assert.Equal("foo_bar", StringUtils.ToSnake("FooBar"));
            Assert.Equal("FOO_BAR", StringUtils.ToSnake("fooBar", true));
            Assert.Equal("FOO_BAR", StringUtils.ToSnake("FooBar", true));

            Assert.Equal("a_b_c", StringUtils.ToSnake("ABC"));
            Assert.Equal("A_B_C", StringUtils.ToSnake("ABC", true));

            Assert.Equal("", StringUtils.ToSnake(""));
            Assert.Equal("", StringUtils.ToSnake("", true));

            var ex = Assert.Throws<ArgumentNullException>(() => StringUtils.ToSnake(null));
            Assert.Contains("Parameter 's'", ex.Message);
        }
    }
}
