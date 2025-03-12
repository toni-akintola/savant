"use client";

import { useState, useEffect, useTransition } from "react";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  MoreHorizontal,
  Search,
  SlidersHorizontal,
  Loader2,
} from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { useDebounce } from "@/app/hooks/use-debounce";
import {
  searchUsers,
  getUserById,
  type User,
  type SearchParams,
  type ValidSortField,
} from "@/app/actions";
import { Skeleton } from "@/components/ui/skeleton";

// Get the valid sort fields from the actions file
// type ValidSortField = "followersCount" | "followsCount" | "postsCount" | "createdAt" | "_id" | "handle" | "displayName";

export function UserProfiles() {
  // Search and filter state
  const [searchQuery, setSearchQuery] = useState("");
  const debouncedSearchQuery = useDebounce(searchQuery, 300);
  const [sortField, setSortField] = useState<ValidSortField>("followersCount");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(10);
  const [isFilterOpen, setIsFilterOpen] = useState(false);
  const [filters, setFilters] = useState({
    minFollowers: "",
    maxFollowers: "",
    minPosts: "",
    maxPosts: "",
  });

  // Data state
  const [users, setUsers] = useState<User[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  const [isDetailOpen, setIsDetailOpen] = useState(false);

  // Loading states
  const [isPending, startTransition] = useTransition();
  const [isLoading, setIsLoading] = useState(true);

  // Fetch users when search params change
  useEffect(() => {
    const fetchUsers = async () => {
      setIsLoading(true);

      const searchParams: SearchParams = {
        query: debouncedSearchQuery,
        page: currentPage,
        limit: itemsPerPage,
        sortField,
        sortDirection,
        minFollowers: filters.minFollowers
          ? Number(filters.minFollowers)
          : undefined,
        maxFollowers: filters.maxFollowers
          ? Number(filters.maxFollowers)
          : undefined,
        minPosts: filters.minPosts ? Number(filters.minPosts) : undefined,
        maxPosts: filters.maxPosts ? Number(filters.maxPosts) : undefined,
      };

      startTransition(async () => {
        try {
          const result = await searchUsers(searchParams);
          setUsers(result.users);
          setTotalCount(result.totalCount);
          setTotalPages(result.totalPages);
        } catch (error) {
          console.error("Error fetching users:", error);
        } finally {
          setIsLoading(false);
        }
      });
    };

    fetchUsers();
  }, [
    debouncedSearchQuery,
    currentPage,
    itemsPerPage,
    sortField,
    sortDirection,
    filters,
  ]);

  const handleSort = (field: ValidSortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
    setCurrentPage(1); // Reset to first page when sorting changes
  };

  const handleViewDetails = async (user: User) => {
    setIsDetailOpen(true);

    startTransition(async () => {
      try {
        // In a real app, we'd fetch the full user details here
        const fullUserDetails = await getUserById(user._id);
        setSelectedUser(fullUserDetails);
      } catch (error) {
        console.error("Error fetching user details:", error);
      }
    });
  };

  const handleFilterChange = (key: string, value: string) => {
    setFilters((prev) => ({
      ...prev,
      [key]: value,
    }));
  };

  const clearFilters = () => {
    setFilters({
      minFollowers: "",
      maxFollowers: "",
      minPosts: "",
      maxPosts: "",
    });
    setIsFilterOpen(false);
    setCurrentPage(1); // Reset to first page when filters are cleared
  };

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat().format(num);
  };

  return (
    <>
      <div className="flex flex-col gap-6 w-full">
        <div className="space-y-5">
          <div>
            <h2 className="text-3xl font-bold tracking-tight">User Profiles</h2>
            <p className="text-muted-foreground mt-1">
              Manage and explore user profiles from your database.
            </p>
          </div>
          <div className="flex items-center gap-4">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
              <Input
                type="search"
                placeholder="Search users by name, handle, or description..."
                className="pl-10 h-10 w-full"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            <Button
              variant="outline"
              size="icon"
              onClick={() => setIsFilterOpen(!isFilterOpen)}
              className={isFilterOpen ? "bg-accent" : ""}
            >
              <SlidersHorizontal className="h-4 w-4" />
              <span className="sr-only">Filter</span>
            </Button>
          </div>
        </div>

        {isFilterOpen && (
          <Card className="w-full">
            <CardContent className="p-6">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Min Followers</label>
                  <Input
                    type="number"
                    placeholder="Min followers"
                    value={filters.minFollowers}
                    onChange={(e) =>
                      handleFilterChange("minFollowers", e.target.value)
                    }
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Max Followers</label>
                  <Input
                    type="number"
                    placeholder="Max followers"
                    value={filters.maxFollowers}
                    onChange={(e) =>
                      handleFilterChange("maxFollowers", e.target.value)
                    }
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Min Posts</label>
                  <Input
                    type="number"
                    placeholder="Min posts"
                    value={filters.minPosts}
                    onChange={(e) =>
                      handleFilterChange("minPosts", e.target.value)
                    }
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Max Posts</label>
                  <Input
                    type="number"
                    placeholder="Max posts"
                    value={filters.maxPosts}
                    onChange={(e) =>
                      handleFilterChange("maxPosts", e.target.value)
                    }
                  />
                </div>
              </div>
              <div className="flex justify-end mt-6 gap-3">
                <Button variant="outline" onClick={clearFilters}>
                  Clear
                </Button>
                <Button onClick={() => setIsFilterOpen(false)}>
                  Apply Filters
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        <Card className="w-full">
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[280px] px-6">
                    Display Name & Handle
                  </TableHead>
                  <TableHead
                    className="cursor-pointer text-center"
                    onClick={() => handleSort("followersCount")}
                  >
                    <div className="flex items-center justify-center">
                      Followers
                      {sortField === "followersCount" && (
                        <ChevronDown
                          className={`ml-1 h-4 w-4 ${sortDirection === "asc" ? "rotate-180" : ""}`}
                        />
                      )}
                    </div>
                  </TableHead>
                  <TableHead
                    className="cursor-pointer text-center"
                    onClick={() => handleSort("followsCount")}
                  >
                    <div className="flex items-center justify-center">
                      Following
                      {sortField === "followsCount" && (
                        <ChevronDown
                          className={`ml-1 h-4 w-4 ${sortDirection === "asc" ? "rotate-180" : ""}`}
                        />
                      )}
                    </div>
                  </TableHead>
                  <TableHead
                    className="cursor-pointer text-center"
                    onClick={() => handleSort("postsCount")}
                  >
                    <div className="flex items-center justify-center">
                      Posts
                      {sortField === "postsCount" && (
                        <ChevronDown
                          className={`ml-1 h-4 w-4 ${sortDirection === "asc" ? "rotate-180" : ""}`}
                        />
                      )}
                    </div>
                  </TableHead>
                  <TableHead className="text-center">Expertise</TableHead>
                  <TableHead
                    className="cursor-pointer text-center"
                    onClick={() => handleSort("createdAt")}
                  >
                    <div className="flex items-center justify-center">
                      Created
                      {sortField === "createdAt" && (
                        <ChevronDown
                          className={`ml-1 h-4 w-4 ${sortDirection === "asc" ? "rotate-180" : ""}`}
                        />
                      )}
                    </div>
                  </TableHead>
                  <TableHead className="text-right px-6">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  // Loading skeleton
                  Array.from({ length: 5 }).map((_, index) => (
                    <TableRow key={index}>
                      <TableCell className="px-6">
                        <div className="flex items-center gap-3">
                          <Skeleton className="h-10 w-10 rounded-full" />
                          <div className="space-y-1">
                            <Skeleton className="h-4 w-32" />
                            <Skeleton className="h-3 w-24" />
                          </div>
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        <Skeleton className="h-4 w-16 mx-auto" />
                      </TableCell>
                      <TableCell className="text-center">
                        <Skeleton className="h-4 w-16 mx-auto" />
                      </TableCell>
                      <TableCell className="text-center">
                        <Skeleton className="h-4 w-16 mx-auto" />
                      </TableCell>
                      <TableCell className="text-center px-4">
                        <div className="flex gap-1 justify-center">
                          <Skeleton className="h-5 w-16 rounded-full" />
                          <Skeleton className="h-5 w-20 rounded-full" />
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        <Skeleton className="h-4 w-24 mx-auto" />
                      </TableCell>
                      <TableCell className="text-right px-6">
                        <Skeleton className="h-8 w-8 rounded-md ml-auto" />
                      </TableCell>
                    </TableRow>
                  ))
                ) : users.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center py-8">
                      No users found. Try adjusting your search or filters.
                    </TableCell>
                  </TableRow>
                ) : (
                  users.map((user) => (
                    <TableRow key={user._id}>
                      <TableCell className="font-medium px-6">
                        <div className="flex items-center gap-3">
                          <Avatar className="h-10 w-10">
                            <AvatarImage
                              src={user.avatar}
                              alt={user.displayName}
                            />
                            <AvatarFallback>
                              {user.displayName.substring(0, 2)}
                            </AvatarFallback>
                          </Avatar>
                          <div>
                            <div className="font-medium text-base">
                              {user.displayName}
                            </div>
                            <div className="text-sm text-muted-foreground">
                              @{user.handle}
                            </div>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        {formatNumber(user.followersCount)}
                      </TableCell>
                      <TableCell className="text-center">
                        {formatNumber(user.followsCount)}
                      </TableCell>
                      <TableCell className="text-center">
                        {formatNumber(user.postsCount)}
                      </TableCell>
                      <TableCell className="text-center px-4">
                        <div className="flex flex-wrap gap-1 justify-center">
                          {user.metadata?.topics_of_expertise
                            ?.slice(0, 3)
                            .map((topic) => (
                              <Badge
                                key={topic}
                                variant="outline"
                                className="text-xs"
                              >
                                {topic}
                              </Badge>
                            ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        {formatDate(user.createdAt)}
                      </TableCell>
                      <TableCell className="text-right px-6">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8"
                            >
                              <MoreHorizontal className="h-4 w-4" />
                              <span className="sr-only">Open menu</span>
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuLabel>Actions</DropdownMenuLabel>
                            <DropdownMenuItem
                              onClick={() => handleViewDetails(user)}
                            >
                              View details
                            </DropdownMenuItem>
                            <DropdownMenuSeparator />
                            <DropdownMenuItem>Edit user</DropdownMenuItem>
                            <DropdownMenuItem>View posts</DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
          <div className="flex items-center justify-between px-6 py-4 border-t">
            <div className="text-sm text-muted-foreground">
              {isLoading ? (
                <Skeleton className="h-4 w-40" />
              ) : (
                <>
                  Showing <strong>{users.length}</strong> of{" "}
                  <strong>{totalCount}</strong> users
                </>
              )}
            </div>
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                size="icon"
                disabled={currentPage === 1 || isLoading}
                onClick={() => handlePageChange(currentPage - 1)}
              >
                {isPending ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <ChevronLeft className="h-4 w-4" />
                )}
              </Button>
              <span className="text-sm font-medium">
                Page {currentPage} of {totalPages}
              </span>
              <Button
                variant="outline"
                size="icon"
                disabled={currentPage >= totalPages || isLoading}
                onClick={() => handlePageChange(currentPage + 1)}
              >
                {isPending ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <ChevronRight className="h-4 w-4" />
                )}
              </Button>
            </div>
          </div>
        </Card>
      </div>

      {/* User Detail Dialog */}
      <Dialog open={isDetailOpen} onOpenChange={setIsDetailOpen}>
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>User Profile</DialogTitle>
            <DialogDescription>
              Detailed information about the selected user.
            </DialogDescription>
          </DialogHeader>

          {!selectedUser ? (
            <div className="space-y-6">
              <div className="relative">
                <Skeleton className="h-32 w-full rounded-md" />
                <div className="absolute -bottom-12 left-4">
                  <Skeleton className="h-24 w-24 rounded-full border-4 border-background" />
                </div>
              </div>
              <div className="pt-14 space-y-4">
                <div>
                  <Skeleton className="h-8 w-48" />
                  <Skeleton className="h-4 w-32 mt-2" />
                </div>
                <Skeleton className="h-16 w-full" />
                <div className="flex gap-4">
                  <Skeleton className="h-6 w-32" />
                  <Skeleton className="h-6 w-32" />
                  <Skeleton className="h-6 w-32" />
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-6">
              <div className="relative">
                <div className="h-32 w-full rounded-md overflow-hidden">
                  <img
                    src={selectedUser.banner || "/placeholder.svg"}
                    alt="Banner"
                    className="w-full h-full object-cover"
                  />
                </div>
                <div className="absolute -bottom-12 left-4">
                  <Avatar className="h-24 w-24 border-4 border-background">
                    <AvatarImage
                      src={selectedUser.avatar}
                      alt={selectedUser.displayName}
                    />
                    <AvatarFallback>
                      {selectedUser.displayName.substring(0, 2)}
                    </AvatarFallback>
                  </Avatar>
                </div>
              </div>

              <div className="pt-14 space-y-4">
                <div>
                  <h3 className="text-2xl font-bold">
                    {selectedUser.displayName}
                  </h3>
                  <p className="text-muted-foreground">
                    @{selectedUser.handle}
                  </p>
                </div>

                <p>{selectedUser.description}</p>

                <div className="flex gap-4">
                  <div>
                    <span className="font-bold">
                      {formatNumber(selectedUser.followersCount)}
                    </span>{" "}
                    Followers
                  </div>
                  <div>
                    <span className="font-bold">
                      {formatNumber(selectedUser.followsCount)}
                    </span>{" "}
                    Following
                  </div>
                  <div>
                    <span className="font-bold">
                      {formatNumber(selectedUser.postsCount)}
                    </span>{" "}
                    Posts
                  </div>
                </div>

                <Separator />

                <Tabs defaultValue="details">
                  <TabsList>
                    <TabsTrigger value="details">Details</TabsTrigger>
                    <TabsTrigger value="metadata">Metadata</TabsTrigger>
                    <TabsTrigger value="technical">Technical</TabsTrigger>
                  </TabsList>
                  <TabsContent value="details" className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <h4 className="text-sm font-medium text-muted-foreground">
                          Created At
                        </h4>
                        <p>{formatDate(selectedUser.createdAt)}</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-muted-foreground">
                          Last Indexed
                        </h4>
                        <p>{formatDate(selectedUser.indexedAt || "")}</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-muted-foreground">
                          SFC Score
                        </h4>
                        <p>{selectedUser.sfc}</p>
                      </div>
                    </div>
                  </TabsContent>
                  <TabsContent value="metadata" className="space-y-4">
                    <div>
                      <h4 className="text-sm font-medium text-muted-foreground">
                        Topics of Expertise
                      </h4>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {selectedUser.metadata?.topics_of_expertise?.map(
                          (topic) => (
                            <Badge key={topic} variant="secondary">
                              {topic}
                            </Badge>
                          ),
                        )}
                      </div>
                    </div>
                  </TabsContent>
                  <TabsContent value="technical" className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <h4 className="text-sm font-medium text-muted-foreground">
                          ID
                        </h4>
                        <p className="font-mono text-xs">{selectedUser._id}</p>
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-muted-foreground">
                          DID
                        </h4>
                        <p className="font-mono text-xs">{selectedUser.did}</p>
                      </div>
                    </div>
                  </TabsContent>
                </Tabs>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
