#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

TEST_CASE("eliasfano_sparse_bitmap", "[public]") {
    SECTION("construct and assign") {
        // test default constructor
        yaef::eliasfano_sparse_bitmap<true> bitmap1;
        REQUIRE(bitmap1.size() == 0);
        REQUIRE(bitmap1.empty());

        // test constructor with allocator
        yaef::eliasfano_sparse_bitmap<true> bitmap2(yaef::details::aligned_allocator<uint8_t, 32>{});
        REQUIRE(bitmap2.size() == 0);
        REQUIRE(bitmap2.empty());

        // test copy constructor
        yaef::eliasfano_sparse_bitmap<true> bitmap3(bitmap2);
        REQUIRE(bitmap3.size() == 0);
        REQUIRE(bitmap3.empty());

        // test move constructor
        yaef::eliasfano_sparse_bitmap<true> bitmap4(std::move(bitmap3));
        REQUIRE(bitmap4.size() == 0);
        REQUIRE(bitmap4.empty());

        // test constructor from plain bitmap data
        uint64_t blocks[] = {0xAA, 0x55};
        yaef::eliasfano_sparse_bitmap<true> bitmap5(blocks, 128);
        REQUIRE(bitmap5.size() == 128);
        REQUIRE(!bitmap5.empty());

        // test copy assign
        bitmap1 = bitmap5;
        REQUIRE(bitmap1.size() == 128);
        REQUIRE(!bitmap1.empty());

        // test move assign
        bitmap2 = std::move(bitmap1);
        REQUIRE(bitmap2.size() == 128);
        REQUIRE(!bitmap2.empty());
    }

    SECTION("query") {
        uint64_t blocks[] = {0xAA, 0x55};
        yaef::eliasfano_sparse_bitmap<true> bitmap(blocks, 128);

        // test at() and operator[]
        REQUIRE(bitmap.at(0) == false);
        REQUIRE(bitmap[1] == true);

        // test count_one() and count_zero()
        REQUIRE(bitmap.count_one() == 8);
        REQUIRE(bitmap.count_zero() == 120);

        // test rank_one() and rank_zero()
        REQUIRE(bitmap.rank_one(64) == 4);
        REQUIRE(bitmap.rank_zero(64) == 60);

        // test select()
        REQUIRE(bitmap.select(3) == 7);

        // test find_first() and find_last()
        REQUIRE(bitmap.find_first() == 1);
        REQUIRE(bitmap.find_last() == 70);
    }

    SECTION("swap") {
        yaef::eliasfano_sparse_bitmap<true> bitmap1;

        uint64_t blocks[] = {0xAA, 0x55};
        yaef::eliasfano_sparse_bitmap<true> bitmap2(blocks, 128);

        bitmap1.swap(bitmap2);
        assert(bitmap1.size() == 128);
        assert(bitmap2.size() == 0);
    }
}